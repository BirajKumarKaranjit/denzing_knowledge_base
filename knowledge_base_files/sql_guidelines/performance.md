---
name: performance
description: "Use when writing queries that need to be fast and efficient on large datasets. Covers index-aware filtering, partition pruning, scan reduction strategies, and common NBA query optimisation patterns. Choose this for queries on large tables like dwh_f_player_boxscore or dwh_f_player_tracking, or when the query involves full season scans, large GROUP BY operations, or multiple large joins."
tags: [performance, optimisation, indexes, query-planning, scan-reduction]
priority: low
---

# Query Performance for NBA Analytics

## Core Optimisation Rules

1. **Filter before joining** — always apply `WHERE season_year = ...` and `game_type = ...`
   before the JOIN to reduce the number of rows being joined.
2. **Use IDs for joins and filters** — `player_id`, `team_id`, `game_id` are indexed.
   Never join or filter on `full_name`, `player_slug`, or `abbreviation`.
3. **Avoid SELECT *** — always name the columns you need; avoids transferring unused data.
4. **Use CTEs for readability AND optimisation** — Postgres materialises CTEs in some versions,
   which can help avoid repeated full scans.

## Filter-First Pattern

```sql
-- ✅ Good: filter season before joining
SELECT bs.player_id, SUM(bs.points)
FROM dwh_f_player_boxscore bs
JOIN dwh_d_games g ON bs.game_id = g.game_id
WHERE g.season_year = '2022'          -- applied before join evaluation
  AND g.game_type   = 'regular'
GROUP BY bs.player_id;

-- ❌ Avoid: joining everything then filtering
SELECT bs.player_id, SUM(bs.points)
FROM dwh_f_player_boxscore bs
JOIN dwh_d_games g ON bs.game_id = g.game_id
GROUP BY bs.player_id, g.season_year
HAVING g.season_year = '2022';        -- filter applied after full aggregation
```

## Indexed Columns (use these in WHERE / JOIN)

| Table | Indexed columns |
|---|---|
| `dwh_f_player_boxscore` | `game_id`, `player_id`, `team_id` |
| `dwh_f_team_boxscore` | `game_id`, `team_id` |
| `dwh_d_games` | `game_id`, `season_year`, `game_type` |
| `dwh_d_players` | `player_id` |
| `dwh_d_teams` | `team_id` |

## Reducing Large Table Scans

```sql
-- For dwh_f_player_tracking (very wide table), only select needed columns
SELECT pt.player_id, pt.speed, pt.distance
FROM dwh_f_player_tracking pt
WHERE pt.game_id = :game_id;   -- always filter by game_id or player_id first
```

## LIMIT for Exploratory Queries

When a query may return many rows, always add a LIMIT:
```sql
ORDER BY points_per_game DESC
LIMIT 20;
```

## EXISTS vs IN for Large Sets

```sql
-- ✅ Prefer EXISTS when checking membership in a large subquery
WHERE EXISTS (
    SELECT 1 FROM dwh_f_player_awards a
    WHERE a.player_id = p.player_id AND a.season = '2022-23'
)

-- ⚠ IN with a large subquery can be slow on older Postgres versions
WHERE player_id IN (SELECT player_id FROM dwh_f_player_awards WHERE season = '2022-23')
```

