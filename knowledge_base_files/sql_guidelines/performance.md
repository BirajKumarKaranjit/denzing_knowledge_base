---
name: performance
description: "Use when the query involves optimizing SQL for large datasets, choosing between EXISTS and IN for subqueries, pushing filters before joins to reduce scan size, avoiding OR joins on large tables, or when query execution is expected to be slow. Supplements the other KB files with execution-efficiency guidance."
tags: [performance, optimization, EXISTS, IN, filter-before-join, index, LIMIT, scan, OR join]
priority: medium
---

# SQL Performance Guidelines

---

## RULE 1 — Filter Before Joining

Apply the most selective filters in the WHERE clause or inside CTEs before joining large tables. This reduces the working dataset for joins.

```sql
-- CORRECT: filter on season_year before joining
SELECT g.game_id, g.game_date, t.full_name
FROM dwh_d_games g
JOIN dwh_d_teams t ON g.home_team_id = t.team_id
WHERE g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
  AND g.game_type ILIKE '%Regular Season%';
```

---

## RULE 2 — Avoid OR Joins on Large Fact Tables

`JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id` creates a non-sargable condition that prevents index use on large tables. When possible, resolve the team_id first in a CTE and then join.

```sql
-- CORRECT: resolve team first, then join
WITH target_team AS (
    SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Boston Celtics%'
)
SELECT g.*
FROM dwh_d_games g
JOIN target_team t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id;
```

Note: The OR join on games is acceptable for game-level queries. It is only a problem (for correctness, not just performance) when used to derive player team membership.

---

## RULE 3 — Use EXISTS for Existence Checks

When checking whether a player has any qualifying game (rather than counting or aggregating), use `EXISTS` — it short-circuits on first match.

```sql
-- CORRECT: stops after first match
SELECT p.player_id, p.full_name
FROM dwh_d_players p
WHERE EXISTS (
    SELECT 1
    FROM dwh_f_player_awards a
    WHERE a.player_id = p.player_id
      AND a.season = (SELECT MAX(season_year) FROM dwh_d_games)
);
```

---

## RULE 4 — LIMIT Only on Final Output

Do not apply LIMIT inside intermediate CTEs used for aggregations, trends, or streak calculations. This silently truncates analytical data and corrupts results. LIMIT belongs only on the final output SELECT.

```sql
-- WRONG: truncates analytical data mid-computation
WITH data AS (SELECT ... FROM dwh_f_player_boxscore LIMIT 1000)

-- CORRECT: limit only on final output
SELECT ... FROM analysis_cte ORDER BY season_year LIMIT 10;
```

---

## RULE 5 — Use player_id / team_id in Joins, Not Name Columns

Joins on name columns like `player_name` or `team_name` are not indexed and scan the full table. Always join on surrogate keys (`player_id`, `team_id`, `game_id`), then filter/display names via the WHERE clause.

```sql
-- CORRECT: join on indexed key, filter by name in WHERE
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id   -- indexed key join
WHERE p.full_name ILIKE '%LeBron%'                    -- name in WHERE
```
