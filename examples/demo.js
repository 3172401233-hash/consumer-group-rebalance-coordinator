'use strict';

// Simulates a consumer group scaling 1 -> 2 -> 3 members, then losing one to
// a crash, under both protocols. Every rebalance prints its assignment diff.
const { Coordinator } = require('../src');

function printDiff(entry) {
  console.log(`\n[rebalance] generation=${entry.generation} protocol=${entry.protocol} reason=${entry.reason}`);
  for (const [member, tps] of Object.entries(entry.added)) {
    console.log(`  + ${member}: ${tps.join(', ')}`);
  }
  for (const [member, tps] of Object.entries(entry.removed)) {
    console.log(`  - ${member}: ${tps.join(', ')}`);
  }
  for (const [member, tps] of Object.entries(entry.pendingRevocation)) {
    console.log(`  ~ ${member} revoking (awaiting ack): ${tps.join(', ')}`);
  }
}

function run(protocol) {
  console.log(`\n=== ${protocol.toUpperCase()} ===`);
  let now = 0;
  const coordinator = new Coordinator({
    groupId: 'demo',
    partitionsByTopic: { events: 6 },
    protocol,
    sessionTimeoutMs: 10_000,
    now: () => now,
  });
  coordinator.on('rebalance', printDiff);

  coordinator.join('consumer-1', ['events']);
  coordinator.join('consumer-2', ['events']);

  if (protocol === 'cooperative') {
    // consumer-1 runs its revocation callback, then acknowledges.
    const view = coordinator.heartbeat('consumer-1');
    console.log(`\nconsumer-1 acknowledges revocation of: ${view.revoking.join(', ')}`);
    coordinator.ackRevocation('consumer-1', view.generation, view.revoking);
  }

  coordinator.join('consumer-3', ['events']);
  if (protocol === 'cooperative') {
    for (const id of ['consumer-1', 'consumer-2']) {
      const view = coordinator.heartbeat(id);
      if (view.revoking.length > 0) {
        console.log(`\n${id} acknowledges revocation of: ${view.revoking.join(', ')}`);
        coordinator.ackRevocation(id, view.generation, view.revoking);
      }
    }
  }

  // consumer-2 crashes (stops heartbeating); the session expires.
  now += 11_000;
  coordinator.heartbeat('consumer-1');
  coordinator.heartbeat('consumer-3');
  const expired = coordinator.tick();
  console.log(`\nsession expired: ${expired.join(', ')}`);

  console.log(`\nfinal assignment (${protocol}):`);
  for (const [member, tps] of Object.entries(coordinator.assignment())) {
    console.log(`  ${member}: ${tps.join(', ')}`);
  }
}

run('eager');
run('cooperative');
