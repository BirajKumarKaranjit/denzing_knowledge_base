---
name: filters
description: "Use when the query involves filtering data based on text lookup, categorical values, numeric ranges, status flags, date/time ranges, or handling NULL values in a Postgres database. Essential for WHERE clauses involving case-insensitive player/team name searches, streak calculations, season scoping, and 'last game' lookups."
tags: [filters, WHERE clause, text lookup, numeric range, NULL handling, name matching, date filters, streak logic]
priority: high
---

# SQL Filtering Guidelines

## Player Name Matching — Always Use ILIKE with Wildcards or UPPER of LIKE if ILIKE not supported.
Player names in the database may include suffixes (Jr., II, III) or slight variations. **Never use `=` for player name lookups.**
```sql
-- CORRECT: catches "Gary Payton II", "Jimmy Butler III", etc.
WHERE p.full_name ILIKE '%Gary Payton%'

-- WRONG: misses suffixed names entirely
WHERE p.full_name = 'Gary Payton'
WHERE p.full_name = 'Jimmy Butler'
```
**Gotcha:** If a name query returns no results, the first suspect is an exact-match filter. Switch to `ILIKE '%name%'`.
---

## "Last Game" Date Queries — Use MAX, Not CURRENT_DATE
When the user asks about a player's or team's "most recent game" or "last night," resolve the date dynamically from the data. **Never hardcode CURRENT_DATE - 1.**
```sql
-- CORRECT: finds the actual latest game in the data
WHERE pb.game_id = (
    SELECT game_id
    FROM dwh_f_player_boxscore
    WHERE player_id = :player_id
    ORDER BY game_date DESC
    LIMIT 1
)

-- WRONG: fails if no game was played yesterday
WHERE g.game_date = CURRENT_DATE - 1
```
---

## Threshold Filters — Use Strict Inequality for Denominators
When filtering for "perfect" ratios (e.g., 100% shooting) or minimum-attempt conditions, ensure the denominator is **strictly greater than zero**, not `>= 0`.
```sql
-- CORRECT: requires at least 1 attempt, prevents 0/0 = "perfect"
WHERE field_goals_attempted > 0
  AND field_goals_made = field_goals_attempted

-- WRONG: 0 attempts counts as "perfect" (0 = 0)
WHERE field_goals_attempted >= 0
  AND field_goals_made = field_goals_attempted
```
---

## Streak Calculation — Never Pre-Filter Matching Rows
**Critical anti-pattern:** Filtering rows that meet a condition (e.g., 30+ points) before calculating consecutive streaks **removes the gaps between qualifying games**, producing false streaks.
```sql
-- CORRECT: keep all games in sequence; use window functions to detect breaks
WITH all_games AS (
    SELECT
        player_id,
        game_date,
        points,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date) AS rn,
        CASE WHEN points >= 30 THEN 1 ELSE 0 END AS qualifies
    FROM dwh_f_player_boxscore
    WHERE player_id = :player_id
),
groups AS (
    SELECT *,
        rn - ROW_NUMBER() OVER (PARTITION BY player_id, qualifies ORDER BY game_date) AS grp
    FROM all_games
)
SELECT MIN(game_date) AS streak_start, MAX(game_date) AS streak_end, COUNT(*) AS streak_length
FROM groups
WHERE qualifies = 1
GROUP BY player_id, grp
ORDER BY streak_length DESC
LIMIT 1;

-- WRONG: filters first, then numbers — gaps vanish, streaks are inflated
SELECT * FROM dwh_f_player_boxscore
WHERE player_id = :player_id AND points >= 30  -- removes non-qualifying games!
ORDER BY game_date;
```
**This applies to all streak types:** consecutive 30-pt games, consecutive games with a made 3, consecutive games with positive plus-minus, back-to-back 40-point games, etc.
---

## Season Scoping — Match Context to the Question
Default season filters should respect the question's timeframe. "This season" ≠ "career." If the user asks about a player who was a rookie in a prior season, do **not** filter for the current season only.
```sql
-- For current season stats
WHERE g.season_year = (SELECT MAX(season_year) FROM dwh_d_games)

-- For a player's rookie season (determine dynamically)
WHERE pts.season_year = (
    SELECT MIN(season_year)
    FROM dwh_f_player_team_seasons
    WHERE player_id = :player_id
)
```
---

## Avoid LIMIT That Truncates Analytical Results

Do not apply arbitrary `LIMIT` clauses (e.g., `LIMIT 1000`) in intermediate CTEs used for trend or streak analysis — this silently drops data and corrupts results.
```sql
-- WRONG: trend analysis over 10 years cut to 1000 rows
WITH data AS (SELECT ... FROM dwh_f_player_boxscore LIMIT 1000)

--CORRECT: apply LIMIT only on the final output if needed
SELECT ... FROM analysis_cte ORDER BY season_year LIMIT 10;
```
---

## NULL Handling
Use `IS NULL` / `IS NOT NULL`; never compare NULL with `=`.
```sql
SELECT player_id, school FROM dwh_d_players WHERE school IS NOT NULL;
```
---

## Anti-Pattern Summary
| Bad Pattern | Fix |
|---|---|
| `full_name = 'Jimmy Butler'` | `full_name ILIKE '%Jimmy Butler%'` |
| `game_date = CURRENT_DATE - 1` | `ORDER BY game_date DESC LIMIT 1` |
| `attempts >= 0` for "perfect" filter | `attempts > 0` |
| Pre-filter rows before streak calc | Keep all rows; use window functions |
| `LIMIT 1000` in analytical CTEs | Remove or move LIMIT to final output |