---
name: aggregations
description: "Use when the query requires summarising, totalling, counting, averaging, or ranking data. Covers GROUP BY patterns, aggregate functions (SUM, AVG, COUNT), per-game calculations, season totals, leaderboard ranking, and window functions. Choose this for queries about scoring leaders, season averages, team totals, top-N rankings, or any stat that aggregates across multiple rows."
tags: [aggregations, group-by, sum, avg, count, rankings, window-functions]
priority: high
---

# Aggregation Patterns for NBA Analytics

## Foundational Rules

- Always use `COUNT(DISTINCT game_id)` for game counts — never `COUNT(*)` which counts rows.
- Always wrap denominators in `NULLIF(denominator, 0)` to guard against division-by-zero.
- Use `ROUND(..., 2)` for percentage columns; `ROUND(..., 1)` for per-game averages.

## Season Per-Game Averages
```sql
SELECT
    p.full_name,
    COUNT(DISTINCT bs.game_id)                                       AS games_played,
    ROUND(SUM(bs.points) / NULLIF(COUNT(DISTINCT bs.game_id), 0), 1) AS points_per_game,
    ROUND(SUM(bs.assists) / NULLIF(COUNT(DISTINCT bs.game_id), 0), 1) AS assists_per_game,
    ROUND(
        SUM(bs.rebounds_offensive + bs.rebounds_defensive)
        / NULLIF(COUNT(DISTINCT bs.game_id), 0), 1
    ) AS rebounds_per_game
FROM dwh_f_player_boxscore bs
JOIN dwh_d_players p ON bs.player_id = p.player_id
JOIN dwh_d_games g ON bs.game_id = g.game_id
WHERE g.season_year = '2022'
  AND g.game_type   = 'regular'
GROUP BY p.player_id, p.full_name
ORDER BY points_per_game DESC;
```

## Season Totals
```sql
SELECT
    p.full_name,
    SUM(bs.points)  AS total_points,
    SUM(bs.assists) AS total_assists
FROM dwh_f_player_boxscore bs
JOIN dwh_d_players p ON bs.player_id = p.player_id
JOIN dwh_d_games g ON bs.game_id = g.game_id
WHERE g.season_year = '2022'
GROUP BY p.player_id, p.full_name;
```

## Top-N Leaderboard with RANK / DENSE_RANK
```sql
WITH season_stats AS (
    SELECT
        p.full_name,
        ROUND(SUM(bs.points) / NULLIF(COUNT(DISTINCT bs.game_id), 0), 1) AS ppg
    FROM dwh_f_player_boxscore bs
    JOIN dwh_d_players p ON bs.player_id = p.player_id
    JOIN dwh_d_games g    ON bs.game_id = g.game_id
    WHERE g.season_year = '2022' AND g.game_type = 'regular'
    GROUP BY p.player_id, p.full_name
)
SELECT full_name, ppg, RANK() OVER (ORDER BY ppg DESC) AS rank
FROM season_stats
WHERE ppg IS NOT NULL
ORDER BY rank
LIMIT 10;
```

## Team-Level Aggregations
```sql
SELECT
    t.full_name AS team_name,
    ROUND(AVG(tbs.points), 1) AS avg_points_per_game
FROM dwh_f_team_boxscore tbs
JOIN dwh_d_teams t   ON tbs.team_id  = t.team_id
JOIN dwh_d_games g   ON tbs.game_id  = g.game_id
WHERE g.season_year = '2022' AND g.game_type = 'regular'
GROUP BY t.team_id, t.full_name
ORDER BY avg_points_per_game DESC;
```

## Minimum Games Filter
Always apply a minimum-games filter for statistical relevance:
```sql
HAVING COUNT(DISTINCT bs.game_id) >= 20
```

