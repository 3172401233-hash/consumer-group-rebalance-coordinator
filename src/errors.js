'use strict';

class CoordinatorError extends Error {
  constructor(code, message) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
  }
}

class UnknownMemberError extends CoordinatorError {
  constructor(memberId) {
    super('UNKNOWN_MEMBER', `member "${memberId}" is not part of the group (it must rejoin)`);
    this.memberId = memberId;
  }
}

class StaleGenerationError extends CoordinatorError {
  constructor(memberId, generation, currentGeneration) {
    super(
      'STALE_GENERATION',
      `member "${memberId}" used generation ${generation}, but the group is at generation ${currentGeneration}`
    );
    this.memberId = memberId;
    this.generation = generation;
    this.currentGeneration = currentGeneration;
  }
}

class IllegalAssignmentError extends CoordinatorError {
  constructor(memberId, tp, reason) {
    super('ILLEGAL_ASSIGNMENT', `member "${memberId}" does not own ${tp}: ${reason}`);
    this.memberId = memberId;
    this.tp = tp;
  }
}

module.exports = {
  CoordinatorError,
  UnknownMemberError,
  StaleGenerationError,
  IllegalAssignmentError,
};
