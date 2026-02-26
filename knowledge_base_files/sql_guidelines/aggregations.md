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

### Important Rule for Consistency Calculation
- For queries involving consistency, stability, reliability, or variance — never rank purely on raw variance or standard deviation.
- Always incorporate sample size as a confidence weight. Compute a weighted score that discounts entities with fewer observations relative to the dataset.
- Derive the minimum meaningful sample size dynamically from the data distribution, never hardcode it.
- Compute consistency on the primary performance metric implied by the query context (e.g., the measure being compared or evaluated). If multiple metrics exist, choose the most representative aggregate performance measure available in the data.