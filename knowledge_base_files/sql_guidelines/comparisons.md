---
name: comparisons
description: "Use when the query involves CASE WHEN expressions, comparing two or more players or teams side-by-side, HAVING threshold filters, conditional aggregations, playoff round ordering, back-to-back or consecutive-date streak comparisons, column existence verification, or any head-to-head analysis. Critical rules: never use MAX() on string columns for ordering, always map playoff rounds to integers, verify column existence before referencing, use LAG() on unfiltered sequences for date-proximity streaks."
tags: [comparisons, CASE WHEN, head-to-head, side-by-side, HAVING, playoff rounds, column validation, back-to-back, LAG, streak, conditional aggregation, FILTER, subquery, head-to-head deduplication]
priority: critical
---

# SQL Comparisons Guide

---

## RULE 1 — Playoff Round Ordering: Map to Integers Before Sorting

Playoff round names are strings. `MAX()` and `ORDER BY` on strings uses alphabetical order, which is entirely wrong for playoff progression (e.g., alphabetically "First Round" > "Finals"). Always map round names to integers using CASE WHEN before applying MAX or ORDER BY.

```sql
-- CORRECT: map to integers first
WITH round_order AS (
    SELECT
        g.game_id,
        g.playoff_round,
        CASE g.playoff_round
            WHEN 'First Round'           THEN 1
            WHEN 'Conference Semifinals' THEN 2
            WHEN 'Conference Finals'     THEN 3
            WHEN 'Finals'                THEN 4
            ELSE 0
        END AS round_num
    FROM dwh_d_games g
    WHERE g.game_type ILIKE '%playoff%'
)
SELECT playoff_round
FROM round_order
GROUP BY playoff_round
ORDER BY MAX(round_num) DESC
LIMIT 1;

-- WRONG: alphabetical order — "First Round" sorts after "Finals"
SELECT MAX(playoff_round) FROM dwh_d_games WHERE game_type ILIKE '%playoff%';
```

---

## RULE 2 — Column Existence: Verify Before Referencing

Before writing any column reference, confirm it exists in the schema. Referencing a non-existent column causes an immediate SQL error. When in doubt, derive the metric from columns you know exist.

**Known wrong → correct column mappings:**

| Wrong column (DO NOT USE) | Correct column or approach |
|---|---|
| `rebounds_chances_offensive` | `rebounds_offensive` |
| `rebounds_chances_defensive` | `rebounds_defensive` |
| `net_rating` | Compute: `AVG(offensive_rating) - AVG(defensive_rating)` |
| `fast_break_points_allowed` | May not exist — verify against schema before using |
| `per` / `player_efficiency_rating` | Verify column exists; if not, do not compute or compute a simplified version labeled "Simple Efficiency Rating" |
| Quarter-level points columns | Verify against schema; do not reference if uncertain |

**Rule:** If a derived metric (PER, net rating, eFG%) is not explicitly stored, compute it from confirmed base columns. If base columns are also unavailable, state clearly that the data is not available rather than hallucinating a column name.

---

## RULE 3 — Back-to-Back / Consecutive-Date Streak Comparisons: Use LAG on Full Sequence

When checking for back-to-back performances (e.g., consecutive 40-point games), use `LAG()` on the full unfiltered game sequence. Never pre-filter qualifying games first — this removes the gaps and makes any two qualifying games appear consecutive.

```sql
-- CORRECT: LAG on full sequence, then check date proximity
WITH all_games AS (
    SELECT
        pb.player_id,
        g.game_date,
        pb.points,
        LAG(g.game_date) OVER (PARTITION BY pb.player_id ORDER BY g.game_date) AS prev_game_date,
        LAG(pb.points)   OVER (PARTITION BY pb.player_id ORDER BY g.game_date) AS prev_points
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Kawhi Leonard%'
      AND g.game_type ILIKE '%Regular Season%'
)
SELECT game_date, points, prev_game_date, prev_points
FROM all_games
WHERE points >= 40
  AND prev_points >= 40
  AND game_date - prev_game_date <= 2;   -- allow one rest day between back-to-backs

-- WRONG: pre-filter removes gaps — any two qualifying games look consecutive
WITH high_games AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY game_date) AS rn
    FROM dwh_f_player_boxscore
    WHERE player_id = :id AND points >= 40   -- gaps removed!
)
SELECT * FROM high_games WHERE rn = LAG(rn) OVER (...) + 1;
```

---

## RULE 4 — Head-to-Head Player Comparison: Deduplicate Join Results

When joining two players' boxscore rows on the same game to compare them side by side, the JOIN produces two rows per game (player A vs player B, and player B vs player A). Deduplicate by adding a WHERE condition so only one ordering is retained.

```sql
-- CORRECT: filter to one canonical ordering using player IDs
SELECT
    pb1.player_id AS player1_id, p1.full_name AS player1,
    pb2.player_id AS player2_id, p2.full_name AS player2,
    AVG(pb1.points) AS p1_avg_pts,
    AVG(pb2.points) AS p2_avg_pts
FROM dwh_f_player_boxscore pb1
JOIN dwh_f_player_boxscore pb2
    ON pb1.game_id = pb2.game_id
   AND pb1.player_id < pb2.player_id          -- deduplication: only one row per game pair
JOIN dwh_d_players p1 ON pb1.player_id = p1.player_id
JOIN dwh_d_players p2 ON pb2.player_id = p2.player_id
WHERE p1.full_name ILIKE '%Jokic%'
  AND p2.full_name ILIKE '%Embiid%'
GROUP BY pb1.player_id, p1.full_name, pb2.player_id, p2.full_name;

-- WRONG: no deduplication — every game appears twice with players swapped
... WHERE (p1.full_name ILIKE '%Embiid%' AND p2.full_name ILIKE '%Jokic%')
       OR (p1.full_name ILIKE '%Jokic%'  AND p2.full_name ILIKE '%Embiid%')
-- produces duplicate rows with identical stats but swapped player labels
```

---

## RULE 5 — Conditional Aggregations Using FILTER

Use the `FILTER` clause for conditional sums/counts within a single GROUP BY query.

```sql
SELECT
    player_id,
    SUM(points) FILTER (WHERE game_type ILIKE '%Regular Season%') AS regular_season_points,
    SUM(points) FILTER (WHERE game_type ILIKE '%playoff%')        AS playoff_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_games g ON pb.game_id = g.game_id
GROUP BY player_id;
```

---

## RULE 6 — HAVING vs WHERE

`HAVING` filters after aggregation. `WHERE` filters before aggregation. Never use HAVING to filter on unaggregated row-level conditions.

```sql
-- CORRECT: HAVING for post-aggregation threshold
SELECT team_id, COUNT(*) AS games_fouled_out
FROM dwh_f_player_boxscore
GROUP BY team_id
HAVING COUNT(*) > 5;

-- Filter on non-aggregated condition belongs in WHERE, not HAVING
WHERE pb.fouls_personal >= 6   -- row-level filter, belongs in WHERE
```

---

## RULE 7 — CASE WHEN Coverage: Always Handle All Cases

When using CASE WHEN for categorization, cover all expected values explicitly. An uncovered case returns NULL silently.

```sql
-- CORRECT: covers all cases including ELSE fallback
CASE
    WHEN player_height > 200 THEN 'Tall'
    WHEN player_height BETWEEN 180 AND 200 THEN 'Average'
    ELSE 'Short'
END AS height_category

-- WRONG: no ELSE — returns NULL for heights outside defined ranges
CASE
    WHEN player_height > 200 THEN 'Tall'
    WHEN player_height > 190 THEN 'Average'
END
```

---

## RULE 8 — Subquery Comparisons: Must Return a Scalar

Subqueries used in comparisons (`>`, `=`, `<`) must return exactly one row and one column. Use `LIMIT 1` or an aggregate function to enforce this.

```sql
-- CORRECT: aggregate ensures scalar result
WHERE home_score > (SELECT AVG(home_score) FROM dwh_d_games)

-- WRONG: subquery may return multiple rows, causing a runtime error
WHERE home_score > (SELECT home_score FROM dwh_d_games WHERE season_year = '2024')
```

---

## RULE 9 — Comparing Player Performance vs Season Average (Elevation Analysis)

When analyzing whether a player "elevates" their game in specific matchups, compute both their head-to-head average and their overall season average and present them side by side.

```sql
WITH h2h AS (
    SELECT pb.player_id, AVG(pb.points) AS h2h_avg_pts
    FROM dwh_f_player_boxscore pb
    -- filtered to games where opponent was specific team (see joins.md for opponent pattern)
    GROUP BY pb.player_id
),
season_avg AS (
    SELECT pb.player_id, AVG(pb.points) AS season_avg_pts
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_games g ON pb.game_id = g.game_id
    WHERE g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
    GROUP BY pb.player_id
)
SELECT
    p.full_name,
    h.h2h_avg_pts,
    s.season_avg_pts,
    h.h2h_avg_pts - s.season_avg_pts AS elevation
FROM h2h h
JOIN season_avg s ON h.player_id = s.player_id
JOIN dwh_d_players p ON h.player_id = p.player_id;
```

---

## Anti-Pattern Summary

| Bad Pattern | Consequence | Fix |
|---|---|---|
| `MAX(playoff_round)` on string | Alphabetical order is wrong | Map to integers with CASE WHEN, then MAX |
| Reference `rebounds_chances_offensive` | Column does not exist → SQL error | Use `rebounds_offensive` |
| Reference `net_rating` column | Column does not exist → SQL error | Compute `offensive_rating - defensive_rating` |
| Pre-filter then check consecutive row numbers | Gaps removed → inflated streaks | Use LAG on full unfiltered sequence |
| `=` for player name in comparisons | Misses accent/suffix variations | Use `ILIKE '%name%'` |
| Duplicate rows in head-to-head join | Inflated averages | Add `pb1.player_id < pb2.player_id` |
| HAVING for row-level conditions | Logical error | Move row-level conditions to WHERE |
| Subquery returning multiple rows in comparison | Runtime error | Add aggregate or LIMIT 1 |
