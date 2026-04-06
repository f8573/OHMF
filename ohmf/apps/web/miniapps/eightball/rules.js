(function attachEightballRules(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  }
  root.OHMFEightballRules = api;
}(typeof globalThis !== "undefined" ? globalThis : this, function createEightballRules() {
  "use strict";

  const TOTAL_OBJECT_BALLS = 7;
  const TOTAL_TARGET_BALLS = 8;

  function sanitizeText(value, limit) {
    const normalized = String(value == null ? "" : value).replace(/[\u0000-\u001f\u007f]/g, "").trim();
    if (!normalized) return "";
    return normalized.slice(0, limit);
  }

  function clampNumber(value, min, max, fallback) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return fallback;
    return Math.max(min, Math.min(max, numeric));
  }

  function assignmentLabel(value) {
    const normalized = sanitizeText(value, 16).toLowerCase();
    if (normalized === "solids") return "solids";
    if (normalized === "stripes") return "stripes";
    return "";
  }

  function seatLabel(player) {
    return sanitizeText(player?.display_name || player?.user_id || "Player", 80) || "Player";
  }

  function ballsLeftForPlayer(player) {
    const objectBallsLeft = clampNumber(player?.object_balls_left, 0, TOTAL_OBJECT_BALLS, TOTAL_OBJECT_BALLS);
    return objectBallsLeft + 1;
  }

  function normalizePlayer(player, index) {
    const normalized = {
      user_id: sanitizeText(player?.user_id, 80) || `seat-${index + 1}`,
      display_name: sanitizeText(player?.display_name || player?.user_id, 80) || `Player ${index + 1}`,
      assignment: assignmentLabel(player?.assignment),
      object_balls_left: clampNumber(player?.object_balls_left, 0, TOTAL_OBJECT_BALLS, TOTAL_OBJECT_BALLS),
      fouls: Math.max(0, Number(player?.fouls || 0)),
    };
    normalized.balls_left = ballsLeftForPlayer(normalized);
    return normalized;
  }

  function appendHistoryEntry(history, text, at) {
    const nextHistory = Array.isArray(history) ? history.slice(-7) : [];
    nextHistory.push({
      text: sanitizeText(text, 220),
      at: sanitizeText(at, 80) || new Date().toISOString(),
    });
    return nextHistory;
  }

  function rackProgress(players, winnerUserId) {
    if (winnerUserId) return 100;
    const totalRemaining = (Array.isArray(players) ? players : []).reduce(
      (sum, player) => sum + clampNumber(player?.balls_left, 0, TOTAL_TARGET_BALLS, TOTAL_TARGET_BALLS),
      0
    );
    const totalTargets = TOTAL_TARGET_BALLS * 2;
    const cleared = Math.max(0, totalTargets - totalRemaining);
    return Math.round((cleared / totalTargets) * 100);
  }

  function normalizeSnapshot(snapshot) {
    const players = Array.isArray(snapshot?.players)
      ? snapshot.players.slice(0, 2).map((player, index) => normalizePlayer(player, index))
      : [];
    const winnerUserId = sanitizeText(snapshot?.winner_user_id, 80);
    const activePlayerId = winnerUserId ? "" : sanitizeText(snapshot?.active_player_id, 80);
    return {
      players,
      active_player_id: activePlayerId,
      winner_user_id: winnerUserId,
      turn_number: Math.max(0, Number(snapshot?.turn_number || 0)),
      rack_progress: rackProgress(players, winnerUserId),
      phase: sanitizeText(snapshot?.phase, 32) || (players.length >= 2 ? "open" : "waiting"),
      last_event: sanitizeText(snapshot?.last_event, 220),
      projected_summary: sanitizeText(snapshot?.projected_summary, 220),
      history: Array.isArray(snapshot?.history)
        ? snapshot.history
            .map((entry) => ({
              text: sanitizeText(entry?.text, 220),
              at: sanitizeText(entry?.at, 80),
            }))
            .filter((entry) => entry.text)
        : [],
      breaking_player_id: sanitizeText(snapshot?.breaking_player_id, 80),
    };
  }

  function buildInitialSnapshot(participants) {
    const players = (Array.isArray(participants) ? participants : [])
      .slice(0, 2)
      .map((player, index) => normalizePlayer(player, index));
    const phase = players.length >= 2 ? "break" : "waiting";
    const summary = players.length >= 2 ? `${seatLabel(players[0])} breaks.` : "Waiting for two players.";
    return {
      players,
      active_player_id: players[0]?.user_id || "",
      winner_user_id: "",
      turn_number: 1,
      rack_progress: 0,
      phase,
      last_event: summary,
      projected_summary: "",
      history: appendHistoryEntry([], summary),
      breaking_player_id: players[0]?.user_id || "",
    };
  }

  function nextPlayerId(snapshot) {
    const players = Array.isArray(snapshot?.players) ? snapshot.players : [];
    if (players.length <= 1) return sanitizeText(snapshot?.active_player_id, 80);
    const activeIndex = players.findIndex((player) => player.user_id === sanitizeText(snapshot?.active_player_id, 80));
    return sanitizeText(players[(activeIndex + 1) % players.length]?.user_id, 80);
  }

  function findPlayer(players, userId) {
    return (Array.isArray(players) ? players : []).find((player) => player.user_id === sanitizeText(userId, 80)) || null;
  }

  function objectBallRemainderText(player) {
    const remaining = clampNumber(player?.object_balls_left, 0, TOTAL_OBJECT_BALLS, TOTAL_OBJECT_BALLS);
    if (remaining === 0) return "Only the 8-ball remains.";
    if (remaining === 1) return "1 object ball and the 8-ball remain.";
    return `${remaining} object balls and the 8-ball remain.`;
  }

  function summarizeState(snapshot) {
    const normalized = normalizeSnapshot(snapshot);
    const winner = findPlayer(normalized.players, normalized.winner_user_id);
    if (winner) {
      return `${seatLabel(winner)} wins the rack.`;
    }
    const active = findPlayer(normalized.players, normalized.active_player_id);
    if (active) {
      return `${seatLabel(active)} to shoot.`;
    }
    return "Waiting for two players.";
  }

  function projectableSummary(snapshot) {
    const normalized = normalizeSnapshot(snapshot);
    return normalized.last_event || summarizeState(normalized);
  }

  function oppositeAssignment(value) {
    return value === "solids" ? "stripes" : "solids";
  }

  function applyPocketCounts(active, opponent, ownBallsPocketed, opponentBallsPocketed) {
    active.object_balls_left = Math.max(0, active.object_balls_left - ownBallsPocketed);
    opponent.object_balls_left = Math.max(0, opponent.object_balls_left - opponentBallsPocketed);
    active.balls_left = ballsLeftForPlayer(active);
    opponent.balls_left = ballsLeftForPlayer(opponent);
  }

  function applyShot(snapshot, shot) {
    const normalized = normalizeSnapshot(snapshot);
    const players = normalized.players.map((player, index) => normalizePlayer(player, index));
    if (!players.length) return buildInitialSnapshot([]);

    const activeIndex = players.findIndex((player) => player.user_id === normalized.active_player_id);
    if (activeIndex < 0) return normalized;

    const active = players[activeIndex];
    const opponent = players[(activeIndex + 1) % players.length];
    const phase = normalized.phase || "open";
    const now = sanitizeText(shot?.timestamp, 80) || new Date().toISOString();
    const cueBallScratch = Boolean(shot?.cueBallScratch || shot?.type === "scratch");
    const eightPocketed = Boolean(shot?.eightPocketed || shot?.type === "eight");
    const ownBallsPocketed = Math.max(0, Number(shot?.ownBallsPocketed ?? (shot?.type === "pocket" ? 1 : 0)) || 0);
    const opponentBallsPocketed = Math.max(0, Number(shot?.opponentBallsPocketed || 0) || 0);
    const otherObjectBallsPocketed = Math.max(
      0,
      Number(shot?.otherObjectBallsPocketed ?? ownBallsPocketed + opponentBallsPocketed) || 0
    );

    let activePlayerId = active.user_id;
    let winnerUserId = "";
    let nextPhase = phase;
    let summary = "";

    if (phase === "break") {
      nextPhase = "open";
      if (cueBallScratch) {
        active.fouls += 1;
        activePlayerId = opponent.user_id;
        summary = `${seatLabel(active)} scratches on the break. Ball in hand for ${seatLabel(opponent)}.`;
      } else if (eightPocketed) {
        if (otherObjectBallsPocketed > 0) {
          summary = `${seatLabel(active)} pockets the 8-ball on the break. The 8-ball is respotted and ${seatLabel(active)} keeps shooting.`;
        } else {
          activePlayerId = opponent.user_id;
          summary = `${seatLabel(active)} pockets the 8-ball on the break. The 8-ball is respotted and ${seatLabel(opponent)} takes over.`;
        }
      } else if (otherObjectBallsPocketed > 0) {
        summary = `${seatLabel(active)} pockets a ball on the break and keeps shooting.`;
      } else {
        activePlayerId = opponent.user_id;
        summary = `${seatLabel(active)} breaks dry. ${seatLabel(opponent)} takes over.`;
      }
    } else if (cueBallScratch && eightPocketed) {
      active.fouls += 1;
      winnerUserId = opponent.user_id;
      activePlayerId = "";
      nextPhase = "finished";
      summary = `${seatLabel(active)} scratches while pocketing the 8-ball. ${seatLabel(opponent)} wins the rack.`;
    } else if (cueBallScratch) {
      active.fouls += 1;
      activePlayerId = opponent.user_id;
      summary = `${seatLabel(active)} scratches. Ball in hand for ${seatLabel(opponent)}.`;
    } else if (eightPocketed) {
      if (active.object_balls_left === 0) {
        winnerUserId = active.user_id;
        activePlayerId = "";
        nextPhase = "finished";
        summary = `${seatLabel(active)} sinks the 8-ball and wins the rack.`;
      } else {
        winnerUserId = opponent.user_id;
        activePlayerId = "";
        nextPhase = "finished";
        summary = `${seatLabel(active)} sinks the 8-ball early. ${seatLabel(opponent)} wins the rack.`;
      }
    } else if (shot?.type === "miss" || (ownBallsPocketed === 0 && opponentBallsPocketed === 0)) {
      activePlayerId = opponent.user_id;
      summary = `${seatLabel(active)} misses. ${seatLabel(opponent)} takes over.`;
    } else {
      let assignedThisTurn = false;
      if (!active.assignment) {
        const claimed = assignmentLabel(shot?.claimGroup) || "solids";
        active.assignment = claimed;
        opponent.assignment = oppositeAssignment(claimed);
        assignedThisTurn = true;
      }
      applyPocketCounts(active, opponent, ownBallsPocketed, opponentBallsPocketed);

      if (ownBallsPocketed > 0) {
        activePlayerId = active.user_id;
        if (assignedThisTurn) {
          summary = `${seatLabel(active)} claims ${active.assignment} and keeps shooting. ${objectBallRemainderText(active)}`;
        } else if (opponentBallsPocketed > 0) {
          summary = `${seatLabel(active)} pockets both groups and keeps shooting. ${objectBallRemainderText(active)}`;
        } else {
          summary = `${seatLabel(active)} pockets a ${active.assignment} ball and keeps shooting. ${objectBallRemainderText(active)}`;
        }
      } else {
        activePlayerId = opponent.user_id;
        summary = `${seatLabel(active)} pockets only ${opponent.assignment || "the wrong group"}. ${seatLabel(opponent)} takes over.`;
      }
    }

    players.forEach((player) => {
      player.assignment = assignmentLabel(player.assignment);
      player.object_balls_left = clampNumber(player.object_balls_left, 0, TOTAL_OBJECT_BALLS, TOTAL_OBJECT_BALLS);
      player.balls_left = ballsLeftForPlayer(player);
    });

    return {
      players,
      active_player_id: winnerUserId ? "" : activePlayerId,
      winner_user_id: winnerUserId,
      turn_number: normalized.turn_number + 1,
      rack_progress: rackProgress(players, winnerUserId),
      phase: nextPhase,
      last_event: summary,
      projected_summary: sanitizeText(normalized.projected_summary, 220),
      history: appendHistoryEntry(normalized.history, summary, now),
      breaking_player_id: normalized.breaking_player_id || players[0]?.user_id || "",
    };
  }

  return {
    TOTAL_OBJECT_BALLS,
    TOTAL_TARGET_BALLS,
    appendHistoryEntry,
    applyShot,
    assignmentLabel,
    ballsLeftForPlayer,
    buildInitialSnapshot,
    nextPlayerId,
    normalizeSnapshot,
    objectBallRemainderText,
    projectableSummary,
    seatLabel,
    summarizeState,
  };
}));
