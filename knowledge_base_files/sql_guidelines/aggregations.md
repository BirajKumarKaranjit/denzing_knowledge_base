---
name: aggregations
description: "Use when the query involves calculating totals, averages, counts, per-game rates, per-36-minute stats, consistency metrics, plus-minus, or streak lengths across dimensions such as games, players, or teams. Covers GROUP BY, window functions, safe division, sample-size-weighted consistency, and correct SUM vs AVG usage."
tags: [aggregations, group by, window functions, safe division, per-36, plus-minus, consistency, triple-double, streak]
priority: high
---

## SUM vs AVG — Use the Right Aggregation
Always match the aggregation to what the user is asking:

| User Phrase | Aggregation |
|---|---|
| "total", "how many points overall", "career points" | `SUM` |
| "per game", "average", "on average" | `AVG` |
| "per 36 minutes" | weighted calculation (see below) |

```sql
--Total plus-minus
SELECT player_id, SUM(plus_minus_points) AS total_plus_minus
FROM dwh_f_player_boxscore GROUP BY player_id;

-- Per-game average plus-minus
SELECT player_id, AVG(plus_minus_points) AS avg_plus_minus_per_game
FROM dwh_f_player_boxscore GROUP BY player_id;

-- WRONG: AVG of plus-minus when "total" was asked
SELECT player_id, AVG(plus_minus_points) FROM ...;  -- returns per-game, not total
```
---
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

## Per-36 Minute Stats — Weight by Minutes Played

Per-36 stats must be **weighted by actual minutes played**, not a simple average of game-level rates. Unweighted averages give garbage-time blowout appearances equal weight to full games.

```sql
-- CORRECT: weight stats by minutes, then scale to 36
SELECT
    player_id,
    SUM(points)   / NULLIF(SUM(minutes_played), 0) * 36 AS pts_per_36,
    SUM(assists)  / NULLIF(SUM(minutes_played), 0) * 36 AS ast_per_36,
    SUM(rebounds_offensive + rebounds_defensive)
                  / NULLIF(SUM(minutes_played), 0) * 36 AS reb_per_36
FROM dwh_f_player_boxscore
WHERE player_id = :player_id
  AND minutes_played > 0;

-- WRONG: treats a 3-minute garbage-time game equally with a 40-minute game
SELECT AVG(points / NULLIF(minutes_played, 0) * 36) FROM ...;
```
---

## Consistency Metrics — Always Weight by Sample Size

Low standard deviation alone does not make a player "consistent" — a player with 3 games has trivially low variance. Always incorporate sample size as a confidence weight.

```sql
-- CORRECT: penalize small samples using coefficient of variation + game count weight
WITH stats AS (
    SELECT
        player_id,
        COUNT(*)            AS games,
        AVG(points)         AS avg_pts,
        STDDEV(points)      AS std_pts,
        AVG(points) / NULLIF(STDDEV(points), 0) AS consistency_ratio
    FROM dwh_f_player_boxscore
    GROUP BY player_id
),
min_sample AS (
    SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY games) AS min_games
    FROM stats
)
SELECT s.player_id, s.avg_pts, s.std_pts, s.games,
       s.consistency_ratio * LEAST(s.games::numeric / ms.min_games, 1) AS weighted_consistency
FROM stats s, min_sample ms
WHERE s.games >= ms.min_games
ORDER BY weighted_consistency DESC;

-- WRONG: lowest std dev wins — players with 3 games dominate
SELECT player_id, STDDEV(points) FROM ... ORDER BY STDDEV(points) ASC LIMIT 1;
```

**Rule:** Derive minimum meaningful sample size dynamically from the data distribution. Never hardcode it.
---

## Triple-Double Detection — Check All Stat Combinations

A triple-double requires **10+ in any 3 of the 5 major categories** (points, rebounds, assists, steals, blocks). Do not limit the check to pts/reb/ast only.

```sql
-- CORRECT: counts all valid combinations
SELECT game_id, player_id,
    (CASE WHEN points    >= 10 THEN 1 ELSE 0 END +
     CASE WHEN (rebounds_offensive + rebounds_defensive) >= 10 THEN 1 ELSE 0 END +
     CASE WHEN assists   >= 10 THEN 1 ELSE 0 END +
     CASE WHEN steals    >= 10 THEN 1 ELSE 0 END +
     CASE WHEN blocks    >= 10 THEN 1 ELSE 0 END) AS double_digit_categories
FROM dwh_f_player_boxscore
WHERE player_id = :player_id
HAVING (CASE WHEN points >= 10 THEN 1 ELSE 0 END + ...) >= 3;

-- WRONG: only checks pts/reb/ast — misses steals/blocks combinations
WHERE points >= 10 AND rebounds >= 10 AND assists >= 10;
```
---
## Player Efficiency Rating (PER)
PER requires league averages, pace adjustment, and normalization.
    - If a per column exists → use it.
    - If not → do not calculate PER.
If computing a simplified formula, label it clearly as "Simple Efficiency Rating", not PER.

## Averages by Group (e.g., Minutes by Age) — Use AVG Not SUM

When the question asks for a rate or average across a dimension, group and average — do not sum.

```sql
-- CORRECT: average minutes per game by player age
SELECT
    EXTRACT(YEAR FROM AGE(g.game_date, p.birthdate))::int AS age,
    AVG(pb.minutes_played) AS avg_minutes
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g ON pb.game_id = g.game_id
GROUP BY age
ORDER BY age;

-- WRONG: SUM gives total minutes across all players of that age, not an average
SELECT age, SUM(minutes_played) FROM ... GROUP BY age;
```
---

## GROUP BY Patterns
All non-aggregated columns in SELECT must appear in GROUP BY.
```sql
SELECT game_id, team_id, SUM(points) AS total_points
FROM dwh_f_team_boxscore
GROUP BY game_id, team_id;
```

## HAVING Clauses

`HAVING` filters after aggregation; `WHERE` filters before.

```sql
SELECT team_id, AVG(player_weight) AS avg_weight
FROM dwh_d_players
GROUP BY team_id
HAVING AVG(player_weight) > 200;
```

## Safe Division with NULLIF

```sql
SELECT
    field_goals_made::numeric / NULLIF(field_goals_attempted, 0) * 100 AS fg_pct
FROM dwh_f_player_boxscore;
```

## Window Functions — Ranking

```sql
SELECT player_id, game_id, points,
    RANK() OVER (PARTITION BY game_id ORDER BY points DESC) AS player_rank
FROM dwh_f_player_boxscore;
```

## Aggregates & Grouping Rules
Rule: When selecting columns alongside aggregate functions:
- All non-aggregated columns must appear in GROUP BY.
- If the column is guaranteed to be a single value (like from a one-row CTE), wrap it in an aggregate (MAX, MIN) to satisfy PostgreSQL.
- Avoid assuming implicit behavior—Postgres enforces this strictly.

### Important Rule for Consistency Calculation
- For queries involving consistency, stability, reliability, or variance — never rank purely on raw variance or standard deviation.
- Always incorporate sample size as a confidence weight. Compute a weighted score that discounts entities with fewer observations relative to the dataset.
- Derive the minimum meaningful sample size dynamically from the data distribution, never hardcode it.
- Compute consistency on the primary performance metric implied by the query context (e.g., the measure being compared or evaluated). If multiple metrics exist, choose the most representative aggregate performance measure available in the data.