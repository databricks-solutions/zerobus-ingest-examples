'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { makeRoll, nextRoll, rollDie, PLAYERS } = require('../src/dice');

test('rollDie stays within 1..sides', () => {
  assert.equal(rollDie(6, () => 0), 1);
  assert.equal(rollDie(6, () => 0.9999), 6);
});

test('makeRoll returns exactly the table columns', () => {
  const now = new Date('2026-01-02T03:04:05.000Z');
  const roll = makeRoll({ player: 'Ada', sides: 20, now, random: () => 0.5 });
  assert.deepEqual(Object.keys(roll).sort(), ['die_sides', 'player_name', 'roll', 'rolled_at']);
  assert.equal(roll.player_name, 'Ada');
  assert.equal(roll.die_sides, 20);
  assert.equal(roll.roll, 11);
  assert.equal(roll.rolled_at, 1767323045000 * 1000);
  assert.equal(typeof roll.roll, 'number');
});

test('nextRoll cycles through the players', () => {
  const players = PLAYERS.map((_, i) => nextRoll(i, { random: () => 0 }).player_name);
  assert.deepEqual(players, PLAYERS);
  assert.equal(nextRoll(PLAYERS.length, { random: () => 0 }).player_name, PLAYERS[0]);
});
