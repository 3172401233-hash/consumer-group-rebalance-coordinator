'use strict';

function topicOf(tp) {
  return tp.slice(0, tp.lastIndexOf('-'));
}

function allPartitions(partitionsByTopic) {
  const tps = [];
  for (const topic of Object.keys(partitionsByTopic).sort()) {
    for (let p = 0; p < partitionsByTopic[topic]; p += 1) {
      tps.push(`${topic}-${p}`);
    }
  }
  return tps;
}

// Sticky assignor: keeps partitions with their current owner when possible and
// only moves the minimum number of partitions needed to balance the load.
function computeTargetAssignment(memberIds, subscriptions, partitionsByTopic, holdings) {
  const sortedMembers = [...memberIds].sort();
  const target = new Map(sortedMembers.map((id) => [id, new Set()]));
  const tps = allPartitions(partitionsByTopic);
  const eligibleFor = (topic) => sortedMembers.filter((id) => subscriptions.get(id).includes(topic));

  const unassigned = [];
  for (const tp of tps) {
    const owner = holdings.get(tp);
    if (owner !== undefined && target.has(owner) && subscriptions.get(owner).includes(topicOf(tp))) {
      target.get(owner).add(tp);
    } else {
      unassigned.push(tp);
    }
  }

  for (const tp of unassigned) {
    const eligible = eligibleFor(topicOf(tp));
    if (eligible.length === 0) continue;
    let best = eligible[0];
    for (const id of eligible) {
      if (target.get(id).size < target.get(best).size) best = id;
    }
    target.get(best).add(tp);
  }

  const tpsByTopic = new Map();
  for (const tp of tps) {
    const topic = topicOf(tp);
    if (!tpsByTopic.has(topic)) tpsByTopic.set(topic, []);
    tpsByTopic.get(topic).push(tp);
  }

  for (const topicTps of tpsByTopic.values()) {
    const eligible = eligibleFor(topicOf(topicTps[0]));
    if (eligible.length < 2) continue;
    for (;;) {
      const load = (id) => topicTps.filter((tp) => target.get(id).has(tp)).length;
      let max = eligible[0];
      let min = eligible[0];
      for (const id of eligible) {
        if (load(id) > load(max)) max = id;
        if (load(id) < load(min)) min = id;
      }
      if (load(max) - load(min) <= 1) break;
      const movable = topicTps.filter((tp) => target.get(max).has(tp)).sort().reverse();
      const tp = movable[0];
      target.get(max).delete(tp);
      target.get(min).add(tp);
    }
  }

  return target;
}

function diffAssignments(before, after) {
  const added = {};
  const removed = {};
  const members = new Set([...before.keys(), ...after.keys()]);
  for (const id of members) {
    const b = before.get(id) || new Set();
    const a = after.get(id) || new Set();
    const add = [...a].filter((tp) => !b.has(tp)).sort();
    const rem = [...b].filter((tp) => !a.has(tp)).sort();
    if (add.length > 0) added[id] = add;
    if (rem.length > 0) removed[id] = rem;
  }
  return { added, removed };
}

module.exports = { computeTargetAssignment, diffAssignments, topicOf };
