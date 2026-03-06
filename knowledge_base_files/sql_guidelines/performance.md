---
name: performance
description: "Use when the query involves optimizing SQL execution — filtering before joining, avoiding LIMIT in analytical CTEs, using COUNT(DISTINCT game_id) to prevent double counting, choosing EXISTS vs IN, or pushing predicates early. Also covers output quality rules: always include player name and game date in any result set, include game context when showing game-level stats."
tags: [performance, optimization, filter-before-join, LIMIT, COUNT DISTINCT, EXISTS, IN, output quality, player name, game date]
priority: high
---

# SQL Performance Guidelines

---

## RULE 1 — Filter Before Joining

Always apply WHERE conditions before joining large tables. This reduces rows processed by the join.

```sql
-- CORRECT: season filter in WHERE before join logic is evaluated
SELECT g.game_id, g.game_date, t.full_name
FROM dwh_d_games g
JOIN dwh_d_teams t ON g.home_team_id = t.team_id
WHERE g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
  AND g.game_type ILIKE '%Regular Season%';

-- CORRECT: pre-resolve team_id in CTE before joining large fact table
WITH target_team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Boston Celtics%')
SELECT pb.*, p.full_name
FROM dwh_f_player_boxscore pb
JOIN target_team t ON pb.team_id = t.team_id
JOIN dwh_d_players p ON pb.player_id = p.player_id;
```

---

## RULE 2 — Never Put LIMIT Inside Analytical CTEs

`LIMIT N` inside an intermediate CTE silently drops data, corrupting trend analysis, streak calculations, and aggregations. Apply LIMIT only on the final SELECT.

```sql
-- WRONG: LIMIT 1000 drops analytical data
WITH trend_data AS (
    SELECT * FROM dwh_f_player_boxscore
    WHERE player_id = :id
    LIMIT 1000   -- arbitrary cutoff corrupts streaks, season counts, etc.
)

-- CORRECT: no LIMIT on analytical CTEs; LIMIT only on final output
WITH all_seasons AS (
    SELECT g.season_year, AVG(pb.points) AS ppg
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_games g ON pb.game_id = g.game_id
    WHERE pb.player_id = :id
    GROUP BY g.season_year
)
SELECT * FROM all_seasons ORDER BY season_year LIMIT 20;
```

**Confirmed failure fixed:** Row 129.

---

## RULE 3 — COUNT(DISTINCT game_id) for Per-Game Calculations

When computing per-game averages from fact tables that may have multiple rows per game (due to joins), use `COUNT(DISTINCT pb.game_id)` as the denominator to prevent inflated counts.

```sql
-- CORRECT: distinct game count
SELECT SUM(pb.points) / NULLIF(COUNT(DISTINCT pb.game_id), 0) AS points_per_game
FROM dwh_f_player_boxscore pb
JOIN dwh_d_games g ON pb.game_id = g.game_id;

-- Potentially wrong: double-counts if join introduces duplicate rows
SUM(pb.points) / COUNT(pb.game_id)
```

---

## RULE 4 — EXISTS vs IN for Large Subsets

Use `EXISTS` when checking for existence; use `IN` for small value lists. For large result sets, EXISTS is more efficient.

```sql
-- EXISTS for checking award existence
SELECT p.full_name FROM dwh_d_players p
WHERE EXISTS (
    SELECT 1 FROM dwh_f_player_awards a
    WHERE a.player_id = p.player_id AND a.season = '2024'
);
```

---

## RULE 5 — Output Quality: Always Include Player Name and Game Date

Every result set showing game-level player stats must include:
- `p.full_name` as `player_name` (never raw `player_id`)
- `g.game_date` (never just `game_id`)

```sql
-- CORRECT
SELECT p.full_name AS player_name, g.game_date, pb.points, pb.assists
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id;

-- WRONG: unreadable output
SELECT pb.player_id, pb.game_id, pb.points FROM dwh_f_player_boxscore pb;
```

---

## RULE 6 — Avoid OR Conditions on Large Fact Tables in JOIN Predicates

OR conditions on foreign keys in large tables prevent index use. Resolve to team_id first using a CTE or subquery.

```sql
-- PREFERRED: resolve team first, then join cleanly
WITH team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Heat%')
SELECT g.* FROM dwh_d_games g, team t
WHERE g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id;

-- ACCEPTABLE but slower at scale: direct OR join
JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
WHERE t.full_name ILIKE '%Heat%';
```

---

## Anti-Pattern Summary

| Bad Pattern | Consequence | Fix |
|---|---|---|
| `LIMIT N` inside analytical CTE | Silently drops data | LIMIT on final output only |
| `COUNT(game_id)` without DISTINCT | Double-counts with joins | `COUNT(DISTINCT game_id)` |
| `SELECT pb.player_id` in output | Unreadable | JOIN players, show `p.full_name` |
| `SELECT pb.game_id` only | No date context | JOIN games, show `g.game_date` |
| Filters applied after large join | Slow execution | Push WHERE conditions before joins |
