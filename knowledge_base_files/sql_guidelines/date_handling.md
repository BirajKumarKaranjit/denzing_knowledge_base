---
name: date_handling
description: "Use when the query involves filtering or grouping by date, season year, game date, or time periods. Covers DATE_TRUNC usage, season year conventions (2022 means 2022-23 season), date range filters, and avoiding CURRENT_DATE anti-patterns. Choose this for any query with words like 'last season', 'in 2022', 'during the season', or any time-based analysis."
tags: [dates, season, date-trunc, time-filter, season-year]
priority: medium
---

# Date Handling for NBA Analytics

## Season Year Convention

> **Critical:** `season_year` stores the STARTING year of the season.
> `'2022'` = the 2022-23 NBA season (Oct 2022 – Jun 2023).
> Always confirm with the user if "2022" means the 2022 calendar year or the 2022-23 season.

```sql
-- Regular season 2022-23
WHERE g.season_year = '2022' AND g.game_type = 'regular'

-- Multiple seasons
WHERE g.season_year IN ('2021', '2022', '2023')

-- Range of seasons
WHERE g.season_year::integer BETWEEN 2019 AND 2022
```

## Game Date Filtering

`game_date` is stored as `DATE` in `dwh_d_games`.

```sql
-- Specific calendar date
WHERE g.game_date = '2022-12-25'

-- Date range (Christmas to New Year's)
WHERE g.game_date BETWEEN '2022-12-25' AND '2022-12-31'

-- All games in a calendar month
WHERE DATE_TRUNC('month', g.game_date) = '2022-12-01'
```

## Grouping by Time Period

```sql
-- Monthly breakdown of player scoring
SELECT
    DATE_TRUNC('month', g.game_date) AS month,
    SUM(bs.points)                   AS total_points
FROM dwh_f_player_boxscore bs
JOIN dwh_d_games g ON bs.game_id = g.game_id
WHERE g.season_year = '2022' AND bs.player_id = :player_id
GROUP BY DATE_TRUNC('month', g.game_date)
ORDER BY month;
```

## Anti-Patterns to Avoid

```sql
-- ❌ NEVER use CURRENT_DATE — data may not be live/current
WHERE g.game_date = CURRENT_DATE

-- ✅ Instead, find the latest available date in the data
WHERE g.game_date = (SELECT MAX(game_date) FROM dwh_d_games)

-- ❌ NEVER cast game_date to text for comparison
WHERE CAST(g.game_date AS TEXT) LIKE '2022%'

-- ✅ Use proper date functions
WHERE EXTRACT(YEAR FROM g.game_date) = 2022
```

## Season vs Calendar Year Queries

```sql
-- "Last season" = most recent season_year in data
WITH latest_season AS (
    SELECT MAX(season_year) AS season_year FROM dwh_d_games
)
SELECT ...
FROM dwh_f_player_boxscore bs
JOIN dwh_d_games g ON bs.game_id = g.game_id
JOIN latest_season ls ON g.season_year = ls.season_year;
```

