---
name: comparisons
description: "Use when the query involves CASE WHEN expressions, subquery comparisons, side-by-side entity comparisons, HAVING threshold filters, conditional aggregations, playoff round ordering, or back-to-back/streak comparisons. Critical rules for avoiding non-existent column references and lexicographic ordering errors on categorical fields."
tags: [SQL, comparisons, CASE WHEN, subqueries, HAVING, FILTER, playoff rounds, column validation, streak]
priority: high
---

# SQL Comparisons Guide

## Playoff Round Ordering — Never Use MAX() on String Columns
Playoff round names (e.g., "First Round", "Conference Semifinals", "Finals") are strings. `MAX()` or `ORDER BY` on these fields uses **alphabetical order**, not playoff progression order.
```sql
-- CORRECT: map round names to numeric order first
WITH round_order AS (
    SELECT game_id, playoff_round,
        CASE playoff_round
            WHEN 'First Round'              THEN 1
            WHEN 'Conference Semifinals'    THEN 2
            WHEN 'Conference Finals'        THEN 3
            WHEN 'Finals'                   THEN 4
            ELSE 0
        END AS round_num
    FROM dwh_d_games
    WHERE game_type = 'Playoffs'
)
SELECT playoff_round, MAX(round_num) AS furthest_round
FROM round_order
GROUP BY playoff_round
ORDER BY MAX(round_num) DESC LIMIT 1;

--WRONG: alphabetical MAX gives "First Round" > "Finals"
SELECT MAX(playoff_round) FROM dwh_d_games WHERE game_type = 'Playoffs';
```
---

## Column Existence — Verify Schema Before Writing SQL

Before referencing any column, confirm it exists in the schema. Common mistakes from benchmarking:

| Wrong Column | Correct Column |
|---|---|
| `rebounds_chances_offensive` | `rebounds_offensive` |
| `rebounds_chances_defensive` | `rebounds_defensive` |
| `net_rating` | Compute as `offensive_rating - defensive_rating` |
| `fast_break_points_allowed` | May not exist — check team boxscore schema |

**Rule:** If a derived metric (PER, net rating, eFG%) is not explicitly stored, compute it from available columns using standard formulas. If the required base columns don't exist either, state that clearly rather than referencing a non-existent column.

```sql
-- Computing net rating from available columns
SELECT team_id,
    AVG(offensive_rating) - AVG(defensive_rating) AS net_rating
FROM dwh_f_team_boxscore
GROUP BY team_id;

-- WRONG: assumes column exists without verification
SELECT net_rating FROM dwh_f_team_boxscore;  -- column may not exist
```
---

## Back-to-Back / Consecutive-Date Streaks

When checking for back-to-back performances (e.g., consecutive 40-point games), use `LAG()` to compare actual game dates — not row numbers on a pre-filtered set.
```sql
-- CORRECT: uses LAG on unfiltered game sequence, then checks date proximity
WITH all_games AS (
    SELECT player_id, game_date, points,
        LAG(game_date) OVER (PARTITION BY player_id ORDER BY game_date) AS prev_game_date,
        LAG(points)    OVER (PARTITION BY player_id ORDER BY game_date) AS prev_points
    FROM dwh_f_player_boxscore
    WHERE player_id = :player_id
)
SELECT game_date, points, prev_game_date, prev_points
FROM all_games
WHERE points >= 40
  AND prev_points >= 40
  AND game_date - prev_game_date <= 2;  -- allow for back-to-back or one rest day

-- WRONG: filters 40-pt games first; consecutive row numbers no longer mean consecutive dates
WITH high_games AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY game_date) AS rn
    FROM dwh_f_player_boxscore
    WHERE player_id = :player_id AND points >= 40  -- gaps removed!
)
SELECT * FROM high_games WHERE rn = LAG(rn) OVER (...) + 1;
```
---

## CASE WHEN Expressions

```sql
SELECT player_id, full_name,
    CASE
        WHEN player_height > 200 THEN 'Tall'
        WHEN player_height BETWEEN 180 AND 200 THEN 'Average'
        ELSE 'Short'
    END AS height_category
FROM dwh_d_players;
```

**Gotchas:** Conditions are evaluated sequentially; cover all cases to avoid unexpected NULLs.

---

## Subquery Comparisons Against Aggregated Values
```sql
SELECT game_id, home_team_id, home_score
FROM dwh_d_games
WHERE home_score > (SELECT AVG(home_score) FROM dwh_d_games);
```

**Gotcha:** Subqueries in comparisons must return a single scalar value.
---

## Conditional Aggregations Using FILTER
```sql
SELECT player_id,
    SUM(points) FILTER (WHERE game_type = 'Regular') AS regular_season_points,
    SUM(points) FILTER (WHERE game_type = 'Playoffs') AS playoff_points
FROM dwh_f_player_boxscore
GROUP BY player_id;
```
---

## Side-by-Side Entity Comparisons
```sql
-- Head-to-head player comparison (e.g., Jokić vs Embiid matchups)
SELECT
    pb1.player_id AS player1_id, p1.full_name AS player1,
    pb2.player_id AS player2_id, p2.full_name AS player2,
    AVG(pb1.points) AS p1_avg_pts, AVG(pb2.points) AS p2_avg_pts
FROM dwh_f_player_boxscore pb1
JOIN dwh_f_player_boxscore pb2 ON pb1.game_id = pb2.game_id
    AND pb1.player_id <> pb2.player_id
JOIN dwh_d_players p1 ON pb1.player_id = p1.player_id
JOIN dwh_d_players p2 ON pb2.player_id = p2.player_id
WHERE p1.full_name ILIKE '%Jokić%' AND p2.full_name ILIKE '%Embiid%'
GROUP BY pb1.player_id, p1.full_name, pb2.player_id, p2.full_name;
```
**Note:** Use `ILIKE` for player names in comparisons — accent variations (Jokić vs Jokic) can cause misses.
---

## Anti-Pattern Summary
| Bad Pattern | Fix |
|---|---|
| `MAX(playoff_round)` on string | Map rounds to integers with CASE WHEN, then MAX |
| Reference non-existent column | Verify schema; compute derived metrics from base columns |
| Filter rows before consecutive-date check | Use LAG() on full game sequence; check date diff |
| `=` for player name in comparison | Use `ILIKE '%name%'` |