const test = require("node:test");
const assert = require("node:assert/strict");

const rules = require("./rules.js");

function participants() {
  return [
    { user_id: "player-1", display_name: "Avery" },
    { user_id: "player-2", display_name: "Jordan" },
  ];
}

test("buildInitialSnapshot seats two players and starts on the break", () => {
  const snapshot = rules.buildInitialSnapshot(participants());

  assert.equal(snapshot.phase, "break");
  assert.equal(snapshot.active_player_id, "player-1");
  assert.equal(snapshot.turn_number, 1);
  assert.equal(snapshot.last_event, "Avery breaks.");
  assert.equal(snapshot.players[0].balls_left, 8);
  assert.equal(snapshot.players[1].balls_left, 8);
});

test("8-ball on the break is respotted and turn stays with the breaker when another ball falls", () => {
  const start = rules.buildInitialSnapshot(participants());
  const next = rules.applyShot(start, {
    type: "eight",
    otherObjectBallsPocketed: 1,
    timestamp: "2026-03-26T10:00:00.000Z",
  });

  assert.equal(next.phase, "open");
  assert.equal(next.active_player_id, "player-1");
  assert.equal(next.winner_user_id, "");
  assert.equal(next.last_event, "Avery pockets the 8-ball on the break. The 8-ball is respotted and Avery keeps shooting.");
});

test("first non-break legal pocket assigns groups and keeps turn", () => {
  const start = rules.buildInitialSnapshot(participants());
  const afterBreak = rules.applyShot(start, {
    type: "miss",
    timestamp: "2026-03-26T10:00:00.000Z",
  });
  const next = rules.applyShot(afterBreak, {
    type: "pocket",
    ownBallsPocketed: 1,
    claimGroup: "stripes",
    timestamp: "2026-03-26T10:01:00.000Z",
  });

  assert.equal(next.players[1].assignment, "stripes");
  assert.equal(next.players[0].assignment, "solids");
  assert.equal(next.active_player_id, "player-2");
  assert.equal(next.players[1].object_balls_left, 6);
  assert.equal(next.last_event, "Jordan claims stripes and keeps shooting. 6 object balls and the 8-ball remain.");
});

test("player keeps turn when pocketing both groups as long as one of their own falls", () => {
  const snapshot = {
    players: [
      { user_id: "player-1", display_name: "Avery", assignment: "solids", object_balls_left: 6, fouls: 0 },
      { user_id: "player-2", display_name: "Jordan", assignment: "stripes", object_balls_left: 7, fouls: 0 },
    ],
    active_player_id: "player-1",
    winner_user_id: "",
    turn_number: 3,
    phase: "open",
    last_event: "Avery to shoot.",
    history: [],
  };
  const next = rules.applyShot(snapshot, {
    type: "pocket",
    ownBallsPocketed: 1,
    opponentBallsPocketed: 1,
    timestamp: "2026-03-26T10:02:00.000Z",
  });

  assert.equal(next.active_player_id, "player-1");
  assert.equal(next.players[0].object_balls_left, 5);
  assert.equal(next.players[1].object_balls_left, 6);
  assert.equal(next.last_event, "Avery pockets both groups and keeps shooting. 5 object balls and the 8-ball remain.");
});

test("early 8-ball loss awards the rack to the opponent", () => {
  const snapshot = {
    players: [
      { user_id: "player-1", display_name: "Avery", assignment: "solids", object_balls_left: 3, fouls: 0 },
      { user_id: "player-2", display_name: "Jordan", assignment: "stripes", object_balls_left: 4, fouls: 0 },
    ],
    active_player_id: "player-1",
    winner_user_id: "",
    turn_number: 6,
    phase: "open",
    last_event: "Avery to shoot.",
    history: [],
  };
  const next = rules.applyShot(snapshot, {
    type: "eight",
    timestamp: "2026-03-26T10:03:00.000Z",
  });

  assert.equal(next.winner_user_id, "player-2");
  assert.equal(next.phase, "finished");
  assert.equal(next.last_event, "Avery sinks the 8-ball early. Jordan wins the rack.");
  assert.equal(rules.projectableSummary(next), "Avery sinks the 8-ball early. Jordan wins the rack.");
});

test("scratch always hands over the table", () => {
  const snapshot = {
    players: [
      { user_id: "player-1", display_name: "Avery", assignment: "solids", object_balls_left: 3, fouls: 0 },
      { user_id: "player-2", display_name: "Jordan", assignment: "stripes", object_balls_left: 2, fouls: 0 },
    ],
    active_player_id: "player-1",
    winner_user_id: "",
    turn_number: 7,
    phase: "open",
    last_event: "Avery to shoot.",
    history: [],
  };
  const next = rules.applyShot(snapshot, {
    type: "scratch",
    timestamp: "2026-03-26T10:04:00.000Z",
  });

  assert.equal(next.active_player_id, "player-2");
  assert.equal(next.players[0].fouls, 1);
  assert.equal(next.last_event, "Avery scratches. Ball in hand for Jordan.");
});
