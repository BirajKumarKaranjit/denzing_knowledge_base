---
name: date_handling
description: "Use when the query involves filtering by date range, grouping by season or time period, extracting date components, finding the latest available game or season, computing player age, back-to-back game detection, or any time-based analysis. Critical rules: never use CURRENT_DATE for sports data, always use g.season_year for season grouping (never DATE_TRUNC), use MAX(game_date) or MAX(season_year) to resolve 'latest', and always cast season_year to integer before arithmetic."
tags: [date, season_year, game_date, DATE_TRUNC, MAX, current season, latest game, age, back-to-back, date range, EXTRACT, BETWEEN, season filter]
priority: high
---

# Date and Time Handling Guidelines

---

## RULE 1 — Never Use CURRENT_DATE for Sports Data

Sports databases have a data cutoff. Using `CURRENT_DATE` to filter "today's" or "yesterday's" games will return empty results whenever no game was played on that exact date. Always resolve latest dates dynamically from the data.

```sql
-- CORRECT: dynamic resolution from data
WHERE g.game_date = (SELECT MAX(game_date) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')

-- WRONG: empty results if no game was played yesterday
WHERE g.game_date = CURRENT_DATE - 1
```

---

## RULE 2 — Season Grouping: Always Use g.season_year, Never DATE_TRUNC('year')

The NBA season spans two calendar years (e.g., Oct 2024 – Jun 2025). Using `DATE_TRUNC('year', game_date)` splits a single season across two groups — games from October–December appear in year 2024, and games from January–June appear in year 2025. This corrupts any season-level aggregation.

Always use `g.season_year` from `dwh_d_games` as the season identifier.

```sql
-- CORRECT: season grouping by season_year
SELECT g.season_year, AVG(pb.plus_minus_points) AS avg_plus_minus
FROM dwh_f_player_boxscore pb
JOIN dwh_d_games g ON pb.game_id = g.game_id
WHERE g.game_type ILIKE '%Regular Season%'
GROUP BY g.season_year
ORDER BY g.season_year DESC;

-- WRONG: calendar-year grouping splits NBA season into two groups
SELECT DATE_TRUNC('year', g.game_date) AS year, AVG(pb.plus_minus_points)
FROM ...
GROUP BY DATE_TRUNC('year', g.game_date);
```

---

## RULE 3 — Current Season Resolution: Use MAX(season_year)

Always resolve "current season", "this season", "this year" dynamically using `MAX(season_year)`. Never hardcode a season string.

```sql
-- CORRECT: dynamic current season
WHERE g.season_year = (
    SELECT MAX(season_year)
    FROM dwh_d_games
    WHERE game_type ILIKE '%Regular Season%'
)

-- WRONG: hardcoded season breaks when a new season starts
WHERE g.season_year = '2024-25'
```

---

## RULE 4 — Previous Season: Subquery with season < MAX

```sql
-- Previous season
WHERE g.season_year = (
    SELECT MAX(season_year)
    FROM dwh_d_games
    WHERE season_year < (SELECT MAX(season_year) FROM dwh_d_games)
      AND game_type ILIKE '%Regular Season%'
)
```

---

## RULE 5 — season_year Arithmetic: Cast to Integer First

`season_year` is stored as text (e.g., `'2024'`). Arithmetic directly on a text field causes a type error. Always cast before arithmetic.

```sql
-- CORRECT: cast to integer before arithmetic
WHERE g.season_year::integer >= (SELECT MAX(season_year)::integer FROM dwh_d_games) - 10

-- WRONG: text - integer → type error
WHERE g.season_year >= (SELECT MAX(season_year) FROM dwh_d_games) - 10
```

---

## RULE 6 — "Last 10 Years" / Multi-Year Range Queries

```sql
-- CORRECT: last 10 seasons using integer cast
WHERE g.season_year::integer >= (SELECT MAX(season_year)::integer FROM dwh_d_games) - 9
  AND g.game_type ILIKE '%Regular Season%'
```

---

## RULE 7 — Latest Game per Player or Team

```sql
-- Latest game for a player
WHERE pb.game_id = (
    SELECT pb2.game_id
    FROM dwh_f_player_boxscore pb2
    JOIN dwh_d_games g2 ON pb2.game_id = g2.game_id
    WHERE pb2.player_id = pb.player_id
    ORDER BY g2.game_date DESC
    LIMIT 1
)

-- Latest game for a team
WHERE g.game_date = (
    SELECT MAX(g2.game_date)
    FROM dwh_d_games g2
    WHERE (g2.home_team_id = :team_id OR g2.visitor_team_id = :team_id)
      AND g2.game_type ILIKE '%Regular Season%'
)
ORDER BY g.game_date DESC
LIMIT 1
```

---

## RULE 8 — Back-to-Back Game Detection

Use LAG on `game_date` to compute the gap between consecutive games. A game is "back-to-back" if it occurs within 2 days of the previous game (accounting for travel days).

```sql
WITH team_games AS (
    SELECT
        g.game_id,
        g.game_date,
        LAG(g.game_date) OVER (
            PARTITION BY :team_id
            ORDER BY g.game_date
        ) AS prev_game_date
    FROM dwh_d_games g
    WHERE (g.home_team_id = :team_id OR g.visitor_team_id = :team_id)
      AND g.game_type ILIKE '%Regular Season%'
)
SELECT game_id, game_date, prev_game_date,
    (game_date - prev_game_date) AS days_rest
FROM team_games
WHERE game_date - prev_game_date <= 1;   -- back-to-back = 1 day gap
```

---

## RULE 9 — Player Age Computation

Use `EXTRACT(YEAR FROM AGE(game_date, birthdate))` to compute a player's age at the time of each game. Never hardcode ages or use `EXTRACT(YEAR FROM game_date) - birth_year` alone — this ignores the month/day and is off by up to a year.

```sql
SELECT
    EXTRACT(YEAR FROM AGE(g.game_date, p.birthdate))::int AS age_at_game,
    AVG(pb.minutes_played) AS avg_minutes
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
GROUP BY age_at_game
ORDER BY age_at_game;
```

---

## RULE 10 — Date Range Filtering with BETWEEN

```sql
-- Filter games within a calendar date range
WHERE g.game_date BETWEEN '2024-01-01' AND '2024-12-31'
```

`game_date` is a TIMESTAMP — use `DATE_TRUNC('day', g.game_date)` or `CAST(g.game_date AS DATE)` when comparing to date literals to avoid timestamp boundary issues.

```sql
-- Safe date comparison for TIMESTAMP columns
WHERE CAST(g.game_date AS DATE) BETWEEN '2024-01-01' AND '2024-12-31'
```

---

## RULE 11 — Monthly Analysis: Use DATE_TRUNC for Sub-Season Grouping

For grouping within a season by month (not season-level), `DATE_TRUNC('month', game_date)` is correct and appropriate.

```sql
SELECT
    DATE_TRUNC('month', g.game_date) AS month,
    AVG(pb.points) AS avg_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_games g ON pb.game_id = g.game_id
WHERE g.game_type ILIKE '%Regular Season%'
GROUP BY DATE_TRUNC('month', g.game_date)
ORDER BY month;
```

---

## Quick-Reference Date Patterns

| Use case | Pattern |
|---|---|
| Current season | `g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')` |
| Previous season | `g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE season_year < (SELECT MAX(season_year) FROM dwh_d_games) AND game_type ILIKE '%Regular Season%')` |
| Last N seasons | `g.season_year::integer >= (SELECT MAX(season_year)::integer FROM dwh_d_games) - (N-1)` |
| Latest game | `ORDER BY g.game_date DESC LIMIT 1` |
| Season grouping | `GROUP BY g.season_year` |
| Monthly grouping | `GROUP BY DATE_TRUNC('month', g.game_date)` |
| Player age at game | `EXTRACT(YEAR FROM AGE(g.game_date, p.birthdate))::int` |
| Date arithmetic | `game_date - prev_game_date` (returns interval in days for DATE columns) |

---

## Anti-Pattern Summary

| Bad Pattern | Consequence | Fix |
|---|---|---|
| `CURRENT_DATE - 1` for last game | Empty result if no game yesterday | `ORDER BY game_date DESC LIMIT 1` |
| `DATE_TRUNC('year', game_date)` for season | Splits NBA season across two calendar years | Use `g.season_year` |
| Hardcoded season year string | Breaks when new season starts | `MAX(season_year)` dynamic resolution |
| `season_year - 1` on text field | Type error | `season_year::integer - 1` |
| `EXTRACT(YEAR FROM game_date) - birth_year` for age | Off by up to 1 year | `EXTRACT(YEAR FROM AGE(game_date, birthdate))` |
