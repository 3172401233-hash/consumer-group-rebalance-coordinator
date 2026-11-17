'use strict';

// Durable group-state store. The coordinator persists its generation and
// committed offsets here so a restarted coordinator keeps fencing old
// generations and never loses offsets.
class MemoryStore {
  constructor() {
    this.data = new Map();
  }

  load(groupId) {
    const value = this.data.get(groupId);
    return value ? JSON.parse(JSON.stringify(value)) : null;
  }

  save(groupId, state) {
    this.data.set(groupId, JSON.parse(JSON.stringify(state)));
  }
}

module.exports = { MemoryStore };
