'use strict';

const { EventEmitter } = require('node:events');
const { computeTargetAssignment, diffAssignments } = require('./assignor');
const { UnknownMemberError, StaleGenerationError, IllegalAssignmentError } = require('./errors');

function setEqual(a, b) {
  if (a.size !== b.size) return false;
  for (const v of a) if (!b.has(v)) return false;
  return true;
}

// Group coordinator. Members keep a session alive via heartbeat() and report
// their subscription via join(). The coordinator computes a target assignment
// per generation. With the "cooperative" protocol a partition only moves to a
// new owner after the old owner acknowledges its revocation; with "eager" the
// whole assignment is swapped in a single rebalance. Any commit or response
// carrying an old generation is rejected.
class Coordinator extends EventEmitter {
  constructor({
    groupId,
    partitionsByTopic,
    protocol = 'cooperative',
    sessionTimeoutMs = 10000,
    store = null,
    now = () => Date.now(),
  }) {
    super();
    if (protocol !== 'cooperative' && protocol !== 'eager') {
      throw new Error(`unknown protocol "${protocol}"`);
    }
    this.groupId = groupId;
    this.partitionsByTopic = partitionsByTopic;
    this.protocol = protocol;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.store = store;
    this.now = now;
    this.members = new Map();
    this.generation = 0;
    this.offsets = new Map();
    this.rebalanceLog = [];
    if (this.store) {
      const saved = this.store.load(groupId);
      if (saved) {
        this.generation = saved.generation;
        this.offsets = new Map(Object.entries(saved.offsets));
      }
    }
  }

  join(memberId, topics) {
    const uniqueTopics = [...new Set(topics)].sort();
    const existing = this.members.get(memberId);
    if (existing) {
      existing.lastHeartbeat = this.now();
      const same =
        existing.topics.length === uniqueTopics.length &&
        existing.topics.every((t, i) => t === uniqueTopics[i]);
      if (same) return this.#view(existing);
      existing.topics = uniqueTopics;
      this.#rebalance('subscription_changed');
      return this.#view(existing);
    }
    const member = {
      id: memberId,
      topics: uniqueTopics,
      lastHeartbeat: this.now(),
      owned: new Set(),
      revoking: new Set(),
    };
    this.members.set(memberId, member);
    this.#rebalance('member_joined');
    return this.#view(member);
  }

  heartbeat(memberId) {
    const member = this.#requireMember(memberId);
    member.lastHeartbeat = this.now();
    return this.#view(member);
  }

  // The old owner confirms it has completed its revocation callback and
  // stopped fetching/committing for the given partitions.
  ackRevocation(memberId, generation, partitions) {
    const member = this.#requireMember(memberId);
    if (generation !== this.generation) {
      throw new StaleGenerationError(memberId, generation, this.generation);
    }
    for (const tp of partitions) {
      if (!member.revoking.has(tp)) {
        throw new IllegalAssignmentError(memberId, tp, 'partition is not pending revocation');
      }
    }
    for (const tp of partitions) member.revoking.delete(tp);
    this.#rebalance('revocation_acknowledged');
    return this.#view(member);
  }

  // offsets: [{ topic, partition, offset }]
  commit(memberId, generation, offsets) {
    const member = this.#requireMember(memberId);
    if (generation !== this.generation) {
      throw new StaleGenerationError(memberId, generation, this.generation);
    }
    for (const { topic, partition } of offsets) {
      const tp = `${topic}-${partition}`;
      if (!member.owned.has(tp)) {
        const reason = member.revoking.has(tp)
          ? 'revocation not acknowledged yet'
          : 'partition is assigned to another member';
        throw new IllegalAssignmentError(memberId, tp, reason);
      }
    }
    for (const { topic, partition, offset } of offsets) {
      this.offsets.set(`${topic}-${partition}`, { offset, memberId, generation });
    }
    this.#persist();
  }

  leave(memberId) {
    if (!this.members.delete(memberId)) return;
    this.#rebalance('member_left');
  }

  // Expire members whose session timed out. Their partitions become
  // assignable immediately: a dead owner cannot acknowledge a revocation.
  tick(nowMs = this.now()) {
    const expired = [];
    for (const member of this.members.values()) {
      if (nowMs - member.lastHeartbeat > this.sessionTimeoutMs) expired.push(member.id);
    }
    for (const id of expired) this.members.delete(id);
    if (expired.length > 0) this.#rebalance('member_expired');
    return expired;
  }

  assignment() {
    const view = {};
    for (const member of this.members.values()) {
      view[member.id] = [...member.owned].sort();
    }
    return view;
  }

  committedOffset(topic, partition) {
    const entry = this.offsets.get(`${topic}-${partition}`);
    return entry ? entry.offset : null;
  }

  #requireMember(memberId) {
    const member = this.members.get(memberId);
    if (!member) throw new UnknownMemberError(memberId);
    return member;
  }

  #view(member) {
    return {
      memberId: member.id,
      generation: this.generation,
      assignment: [...member.owned].sort(),
      revoking: [...member.revoking].sort(),
      state: member.revoking.size > 0 ? 'awaiting_revoke' : 'stable',
    };
  }

  #snapshot() {
    const owned = new Map();
    const revoking = new Map();
    for (const member of this.members.values()) {
      owned.set(member.id, new Set(member.owned));
      revoking.set(member.id, new Set(member.revoking));
    }
    return { owned, revoking };
  }

  #rebalance(reason) {
    const holdings = new Map();
    for (const member of this.members.values()) {
      for (const tp of member.owned) holdings.set(tp, member.id);
      for (const tp of member.revoking) holdings.set(tp, member.id);
    }
    const subscriptions = new Map(
      [...this.members.values()].map((m) => [m.id, m.topics])
    );
    const target = computeTargetAssignment(
      [...this.members.keys()],
      subscriptions,
      this.partitionsByTopic,
      holdings
    );

    const before = this.#snapshot();
    const next = new Map();
    let changed = false;

    for (const member of this.members.values()) {
      const want = target.get(member.id);
      const nextOwned = new Set();
      const nextRevoking = new Set();

      if (this.protocol === 'eager') {
        for (const tp of want) nextOwned.add(tp);
      } else {
        for (const tp of member.owned) {
          if (want.has(tp)) nextOwned.add(tp);
          else nextRevoking.add(tp);
        }
        for (const tp of member.revoking) {
          if (want.has(tp)) nextOwned.add(tp); // revocation cancelled
          else nextRevoking.add(tp);
        }
        // A partition moves to its new owner only once it is no longer held
        // by anyone (revocation acked, or previous owner gone).
        for (const tp of want) {
          const holder = holdings.get(tp);
          if (!nextOwned.has(tp) && (holder === undefined || holder === member.id)) {
            nextOwned.add(tp);
          }
        }
      }

      if (!setEqual(nextOwned, member.owned) || !setEqual(nextRevoking, member.revoking)) {
        changed = true;
      }
      next.set(member.id, { owned: nextOwned, revoking: nextRevoking });
    }

    if (!changed) return null;

    this.generation += 1;
    for (const member of this.members.values()) {
      const n = next.get(member.id);
      member.owned = n.owned;
      member.revoking = n.revoking;
    }

    const after = this.#snapshot();
    const { added, removed } = diffAssignments(before.owned, after.owned);
    const pendingRevocation = {};
    for (const [id, tps] of after.revoking) {
      if (tps.size > 0) pendingRevocation[id] = [...tps].sort();
    }
    const entry = {
      generation: this.generation,
      protocol: this.protocol,
      reason,
      added,
      removed,
      pendingRevocation,
    };
    this.rebalanceLog.push(entry);
    this.#persist();
    this.emit('rebalance', entry);
    return entry;
  }

  #persist() {
    if (!this.store) return;
    this.store.save(this.groupId, {
      generation: this.generation,
      offsets: Object.fromEntries(this.offsets),
    });
  }
}

module.exports = { Coordinator };
