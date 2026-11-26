'use strict';

const PLAYERS = ['Ada', 'Grace', 'Linus'];

// Returns a whole number from 1 to sides. `random` returns a number in [0, 1).
function rollDie(sides, random = Math.random) {
  return Math.floor(random() * sides) + 1;
}

// One dice roll as a JSON-ready object whose keys match the table columns.
// A JSON number for a TIMESTAMP column is epoch microseconds (see the README), so rolled_at
// is the current time in microseconds. A string such as an ISO 8601 date is rejected.
function makeRoll({ player, sides = 6, now = new Date(), random = Math.random }) {
  return {
    player_name: player,
    die_sides: sides,
    roll: rollDie(sides, random),
    rolled_at: now.getTime() * 1000,
  };
}

function nextRoll(index, { sides = 6, now = new Date(), random = Math.random } = {}) {
  const player = PLAYERS[index % PLAYERS.length];
  return makeRoll({ player, sides, now, random });
}

module.exports = { PLAYERS, rollDie, makeRoll, nextRoll };
