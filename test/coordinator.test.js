'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  Coordinator,
  MemoryStore,
  UnknownMemberError,
  StaleGenerationError,
  IllegalAssignmentError,
} = require('../src');

function makeClock(start = 1_000_000) {
  const clock = { now: start };
  clock.fn = () => clock.now;
  clock.advance = (ms) => {
    clock.now += ms;
  };
  return clock;
}

function makeCoordinator(overrides = {}) {
  const clock = makeClock();
  const coordinator = new Coordinator({
    groupId: 'g1',
    partitionsByTopic: { t: 6 },
    protocol: 'cooperative',
    sessionTimeoutMs: 10_000,
    now: clock.fn,
    ...overrides,
  });
  return { coordinator, clock };
}

test('cooperative scale-out: new member waits for revocation ack before receiving partitions', () => {
  const { coordinator } = makeCoordinator();

  const a = coordinator.join('A', ['t']);
  assert.equal(a.generation, 1);
  assert.equal(a.assignment.length, 6);

  const b = coordinator.join('B', ['t']);
  assert.equal(b.generation, 2);
  // B gets nothing yet: the 3 moving partitions are still held by A.
  assert.deepEqual(b.assignment, []);

  const aView = coordinator.heartbeat('A');
  assert.equal(aView.state, 'awaiting_revoke');
  assert.equal(aView.assignment.length, 3);
  assert.equal(aView.revoking.length, 3);

  // The reported bug: committing with the NEW generation for a partition
  // whose revocation callback has not completed must be rejected.
  assert.throws(
    () => coordinator.commit('A', 2, [{ topic: 't', partition: 5, offset: 10 }]),
    (err) => err instanceof IllegalAssignmentError && /revocation not acknowledged/.test(err.message)
  );
  // The new member cannot commit for partitions it does not own yet either.
  assert.throws(
    () => coordinator.commit('B', 2, [{ topic: 't', partition: 5, offset: 10 }]),
    IllegalAssignmentError
  );
  // Old-generation commits are fenced.
  assert.throws(
    () => coordinator.commit('A', 1, [{ topic: 't', partition: 0, offset: 10 }]),
    StaleGenerationError
  );
  // A can still commit for the partitions it keeps.
  coordinator.commit('A', 2, [{ topic: 't', partition: 0, offset: 42 }]);

  // Old-generation responses (e.g. a late revocation ack) are rejected too.
  const revoking = aView.revoking;
  assert.throws(() => coordinator.ackRevocation('A', 1, revoking), StaleGenerationError);

  // Once A acknowledges, a follow-up rebalance hands the partitions to B.
  coordinator.ackRevocation('A', 2, revoking);
  const bAfter = coordinator.heartbeat('B');
  assert.equal(bAfter.generation, 3);
  assert.deepEqual(bAfter.assignment.sort(), revoking);
  assert.equal(coordinator.heartbeat('A').state, 'stable');

  // After the follow-up rebalance, generation 2 is stale as well.
  assert.throws(
    () => coordinator.commit('B', 2, [{ topic: 't', partition: 5, offset: 11 }]),
    StaleGenerationError
  );
  coordinator.commit('B', 3, [{ topic: 't', partition: 5, offset: 11 }]);
});

test('rebalance log records the assignment diff of every rebalance', () => {
  const { coordinator } = makeCoordinator();
  coordinator.join('A', ['t']);
  coordinator.join('B', ['t']);
  const revoking = coordinator.heartbeat('A').revoking;
  coordinator.ackRevocation('A', 2, revoking);

  assert.equal(coordinator.rebalanceLog.length, 3);

  const [first, second, third] = coordinator.rebalanceLog;
  assert.equal(first.generation, 1);
  assert.deepEqual(first.added.A, ['t-0', 't-1', 't-2', 't-3', 't-4', 't-5']);

  assert.equal(second.generation, 2);
  assert.deepEqual(second.removed.A, ['t-3', 't-4', 't-5']);
  assert.deepEqual(second.pendingRevocation.A, ['t-3', 't-4', 't-5']);
  assert.deepEqual(second.added, {});

  assert.equal(third.generation, 3);
  assert.deepEqual(third.added.B, ['t-3', 't-4', 't-5']);
  assert.deepEqual(third.pendingRevocation, {});
});

test('member crash: session expiry frees partitions without a revocation ack', () => {
  const { coordinator, clock } = makeCoordinator();
  coordinator.join('A', ['t']);
  coordinator.join('B', ['t']);
  coordinator.ackRevocation('A', 2, coordinator.heartbeat('A').revoking);
  assert.equal(coordinator.heartbeat('B').assignment.length, 3);

  // B crashes: no more heartbeats. Keep A alive.
  clock.advance(6_000);
  coordinator.heartbeat('A');
  clock.advance(6_000);
  coordinator.heartbeat('A');

  const expired = coordinator.tick();
  assert.deepEqual(expired, ['B']);

  const aView = coordinator.heartbeat('A');
  assert.equal(aView.assignment.length, 6);
  assert.equal(aView.state, 'stable');

  // The crashed member is fenced out completely.
  assert.throws(() => coordinator.heartbeat('B'), UnknownMemberError);
  assert.throws(
    () => coordinator.commit('B', coordinator.generation, [{ topic: 't', partition: 3, offset: 1 }]),
    UnknownMemberError
  );
});

test('duplicate join with the same subscription is a no-op', () => {
  const { coordinator } = makeCoordinator();
  coordinator.join('A', ['t']);
  coordinator.join('B', ['t']);
  const logLength = coordinator.rebalanceLog.length;
  const generation = coordinator.generation;

  const again = coordinator.join('A', ['t']);
  assert.equal(again.generation, generation);
  assert.equal(coordinator.rebalanceLog.length, logLength);
  assert.deepEqual(again.assignment, coordinator.heartbeat('A').assignment);
});

test('subscription change triggers a rebalance for the new topics', () => {
  const { coordinator } = makeCoordinator({
    partitionsByTopic: { t1: 2, t2: 4 },
  });
  const a = coordinator.join('A', ['t1']);
  assert.deepEqual(a.assignment, ['t1-0', 't1-1']);

  const changed = coordinator.join('A', ['t1', 't2']);
  assert.equal(changed.generation, 2);
  assert.equal(changed.assignment.length, 6);
  assert.equal(coordinator.rebalanceLog.at(-1).reason, 'subscription_changed');

  // Narrowing the subscription revokes the dropped topic.
  const narrowed = coordinator.join('A', ['t1']);
  assert.equal(narrowed.assignment.length, 2);
});

test('coordinator restart: generation and offsets survive, members must rejoin', () => {
  const store = new MemoryStore();
  const clock = makeClock();
  const base = {
    groupId: 'g1',
    partitionsByTopic: { t: 4 },
    sessionTimeoutMs: 10_000,
    now: clock.fn,
    store,
  };

  const first = new Coordinator({ ...base, protocol: 'eager' });
  first.join('A', ['t']);
  first.commit('A', 1, [{ topic: 't', partition: 0, offset: 42 }]);
  const generationBeforeCrash = first.generation;

  // Coordinator restarts with the same durable store.
  const second = new Coordinator({ ...base, protocol: 'eager' });
  assert.equal(second.generation, generationBeforeCrash);
  assert.equal(second.committedOffset('t', 0), 42);

  // The old member's session is gone: commits and heartbeats are rejected.
  assert.throws(
    () => second.commit('A', generationBeforeCrash, [{ topic: 't', partition: 0, offset: 43 }]),
    UnknownMemberError
  );

  // After rejoining, the member gets its assignment under a new generation
  // and the pre-restart generation stays fenced.
  const rejoined = second.join('A', ['t']);
  assert.equal(rejoined.generation, generationBeforeCrash + 1);
  assert.equal(rejoined.assignment.length, 4);
  assert.throws(
    () => second.commit('A', generationBeforeCrash, [{ topic: 't', partition: 0, offset: 43 }]),
    StaleGenerationError
  );
  second.commit('A', rejoined.generation, [{ topic: 't', partition: 0, offset: 43 }]);
  assert.equal(second.committedOffset('t', 0), 43);
});

test('eager protocol reassigns everything in a single rebalance', () => {
  const { coordinator } = makeCoordinator({ protocol: 'eager' });
  coordinator.join('A', ['t']);
  const b = coordinator.join('B', ['t']);

  // No revocation phase: B immediately owns its share.
  assert.equal(b.generation, 2);
  assert.deepEqual(b.assignment, ['t-3', 't-4', 't-5']);
  assert.deepEqual(coordinator.heartbeat('A').assignment, ['t-0', 't-1', 't-2']);
  assert.equal(coordinator.heartbeat('A').state, 'stable');

  const entry = coordinator.rebalanceLog.at(-1);
  assert.deepEqual(entry.pendingRevocation, {});
  assert.deepEqual(entry.removed.A, ['t-3', 't-4', 't-5']);
  assert.deepEqual(entry.added.B, ['t-3', 't-4', 't-5']);

  // The old owner is fenced by generation immediately.
  assert.throws(
    () => coordinator.commit('A', 1, [{ topic: 't', partition: 0, offset: 7 }]),
    StaleGenerationError
  );
});

test('cooperative revocation is cancelled if the new owner leaves before the ack', () => {
  const { coordinator } = makeCoordinator();
  coordinator.join('A', ['t']);
  coordinator.join('B', ['t']);
  assert.equal(coordinator.heartbeat('A').revoking.length, 3);

  // B leaves before A acknowledges: A keeps everything.
  coordinator.leave('B');
  const aView = coordinator.heartbeat('A');
  assert.equal(aView.assignment.length, 6);
  assert.equal(aView.state, 'stable');
});
