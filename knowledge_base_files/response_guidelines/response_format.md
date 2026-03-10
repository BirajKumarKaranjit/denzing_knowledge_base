---
name: response_format
description: >
  Use when deciding which columns to include in the final SELECT.
  Apply to any query that returns an aggregate, a scoped result, a single-record
  lookup, a leaderboard, or a comparison — to ensure the output is self-explanatory
  without the user needing to re-read the question to understand what the number means.
  Do NOT use for WHERE clause or JOIN decisions — see filters.md for those.
example_queries:
  - "how many points did Bronny James score in his rookie season"
  - "what were LeBron's stats in his last game"
  - "Curry's scoring average this season"
  - "who leads the league in triple doubles"
  - "compare LeBron and Curry stats this season"
  - "Jokic career totals"
tags: [output, SELECT, columns, completeness, context, scope, sample size]
priority: high
---

# SQL Output Completeness Guidelines

**Core rule:** Never return a bare aggregate without the columns that answer
"aggregate of what, covering what scope, based on how many records?"

---

## 1. CTE-Computed Scope Values Must Appear in Final SELECT

When a CTE computes a boundary value that scopes the main query
(a MIN, MAX, or derived period), expose it in the final SELECT.
Without it, the user sees a number but cannot tell what time period it covers.

```sql
-- BAD: rookie_year scopes the query but is hidden — user sees 87 points, doesn't know which season
WITH rookie_season AS (
    SELECT MIN(g.season_year) AS rookie_year
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_games g ON pb.game_id = g.game_id
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    WHERE p.full_name ILIKE '%Bronny James%'
)
SELECT p.full_name, SUM(pb.points) AS total_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g ON pb.game_id = g.game_id
JOIN rookie_season rs ON g.season_year = rs.rookie_year
WHERE p.full_name ILIKE '%Bronny James%'
GROUP BY p.full_name;

-- GOOD: expose rookie_year and games_played — output is self-explanatory
WITH rookie_season AS (
    SELECT MIN(g.season_year) AS rookie_year
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_games g ON pb.game_id = g.game_id
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    WHERE p.full_name ILIKE '%Bronny James%'
)
SELECT
    p.full_name,
    rs.rookie_year                             AS season,
    COUNT(DISTINCT pb.game_id)                 AS games_played,
    SUM(pb.points)                             AS total_points,
    ROUND(AVG(pb.points)::NUMERIC, 1)          AS ppg
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g ON pb.game_id = g.game_id
JOIN rookie_season rs ON g.season_year = rs.rookie_year
WHERE p.full_name ILIKE '%Bronny James%'
GROUP BY p.full_name, rs.rookie_year;
```

---

## 2. Season Aggregates: Include season_year and games_played

```sql
-- BAD: which season? how many games is the average based on?
SELECT p.full_name, ROUND(AVG(pb.points)::NUMERIC, 1) AS ppg
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g ON pb.game_id = g.game_id
WHERE p.full_name ILIKE '%LeBron James%'
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name;

-- GOOD
SELECT
    p.full_name,
    g.season_year,
    COUNT(DISTINCT pb.game_id)                          AS games_played,
    ROUND(AVG(pb.points)::NUMERIC, 1)                   AS ppg,
    ROUND(AVG(pb.assists)::NUMERIC, 1)                  AS apg,
    ROUND(AVG(pb.rebounds_offensive
              + pb.rebounds_defensive)::NUMERIC, 1)     AS rpg
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g ON pb.game_id = g.game_id
WHERE p.full_name ILIKE '%LeBron James%'
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name, g.season_year;
```

---

## 3. Single-Game Stats: Include game_date and Opponent

```sql
-- BAD: when? against whom?
SELECT p.full_name, pb.points, pb.assists
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g ON pb.game_id = g.game_id
ORDER BY g.game_date DESC LIMIT 1;

-- GOOD
SELECT
    p.full_name,
    g.game_date,
    ht.full_name                                          AS home_team,
    vt.full_name                                          AS visitor_team,
    g.home_score,
    g.visitor_score,
    pb.points,
    pb.assists,
    pb.rebounds_offensive + pb.rebounds_defensive         AS total_rebounds,
    pb.steals,
    pb.blocks,
    pb.turnovers
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p   ON pb.player_id      = p.player_id
JOIN dwh_d_games g     ON pb.game_id        = g.game_id
JOIN dwh_d_teams ht    ON g.home_team_id    = ht.team_id
JOIN dwh_d_teams vt    ON g.visitor_team_id = vt.team_id
WHERE p.full_name ILIKE '%Kevin Durant%'
ORDER BY g.game_date DESC LIMIT 1;
```

---

## Quick Reference

| Query type | Must include | Recommended additions |
|---|---|---|
| Season aggregate | `season_year`, `games_played` | `game_type` |
| Single-game stat | `game_date`, home + visitor team | `home_score`, `visitor_score` |
| Career total | `seasons_played`, `games_played` | `career_high` |
| CTE-scoped query | the CTE's computed scope value | — |
| Leaderboard | `RANK()`, `games_played` | `seasons_in_data` |
| Comparison (A vs B) | identical columns for all entities | `season_year` |