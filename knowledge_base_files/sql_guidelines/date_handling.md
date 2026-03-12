---
name: date_handling
description: "Use when the query involves any time-based filtering or grouping — season grouping, last game resolution, last N games, season year arithmetic, back-to-back date detection, player age calculation, month filtering, or current/previous/rookie season dynamic resolution. Key rules: always use g.season_year for NBA season grouping (never DATE_TRUNC year), always use ORDER BY game_date DESC for last game resolution (never CURRENT_DATE), and always cast season_year to integer before arithmetic."
tags: [date_handling, season_year, DATE_TRUNC, game_date, last game, CURRENT_DATE, back-to-back, player age, EXTRACT, AGE, month filter, current season, previous season, rookie season, season arithmetic]
priority: critical
---

# SQL Date Handling Guidelines

---

## RULE 1 — Season Grouping: Always g.season_year, Never DATE_TRUNC('year')

NBA seasons span two calendar years (Oct 2024 – Jun 2025). `DATE_TRUNC('year', game_date)` splits a single season across two groups: games before Jan 1 land in one group, games after Jan 1 land in another. This corrupts all season-level aggregations.

Always use `g.season_year` from `dwh_d_games` to group by NBA season.

```sql
-- CORRECT: season grouping using season_year
SELECT g.season_year,
    AVG(pb.points)  AS ppg,
    AVG(pb.assists) AS apg
FROM dwh_f_player_boxscore pb
JOIN dwh_d_games g ON pb.game_id = g.game_id
JOIN dwh_d_players p ON pb.player_id = p.player_id
WHERE p.full_name ILIKE '%LeBron James%'
  AND g.game_type ILIKE '%Regular Season%'
GROUP BY g.season_year
ORDER BY g.season_year;

-- WRONG: splits 2024-25 NBA season into two calendar year groups
GROUP BY DATE_TRUNC('year', g.game_date)
```

---

## RULE 2 — Current Season: Dynamic MAX, Never Hardcoded Year

Never hardcode a season year. Always resolve the current season dynamically.

```sql
-- CORRECT: always resolves to the latest season with data
AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')

-- WRONG: breaks as soon as the next season starts
AND g.season_year = '2024'
AND g.season_year = '2024-25'
```

---

## RULE 3 — Previous Season: Dynamic Subquery

```sql
AND g.season_year = (
    SELECT MAX(season_year) FROM dwh_d_games
    WHERE season_year < (SELECT MAX(season_year) FROM dwh_d_games)
      AND game_type ILIKE '%Regular Season%'
)
```

---

## RULE 4 — Rookie Season: Dynamic MIN, Never Hardcoded

Never hardcode a player's rookie year. Determine it dynamically from `dwh_f_player_team_seasons`. A player who was a rookie 2 seasons ago will not be found if you apply the current season filter to them.

```sql
-- Rookie season (for any player)
AND g.season_year = (
    SELECT MIN(pts.season_year) FROM dwh_f_player_team_seasons pts
    WHERE pts.player_id = (
        SELECT player_id FROM dwh_d_players WHERE full_name ILIKE '%Bronny James%'
    )
)
```
---

## RULE 5 — Last Game: ORDER BY game_date DESC LIMIT 1, Never CURRENT_DATE

The database may not have games from today or yesterday. Always resolve "last game" dynamically from the data.

```sql
-- CORRECT: most recent game in the data
WHERE pb.game_id = (
    SELECT pb2.game_id
    FROM dwh_f_player_boxscore pb2
    JOIN dwh_d_games g2 ON pb2.game_id = g2.game_id
    JOIN dwh_d_players p2 ON pb2.player_id = p2.player_id
    WHERE p2.full_name ILIKE '%LeBron James%'
    ORDER BY g2.game_date DESC
    LIMIT 1
)

-- WRONG: fails if no game was played yesterday
WHERE g.game_date = CURRENT_DATE - 1
WHERE g.game_date = CURRENT_DATE
```

---

## RULE 6 — Last N Games: ORDER BY game_date DESC, Never game_id DESC

Game IDs are not guaranteed to be assigned in chronological order. Always order by `game_date DESC` when selecting recent games.

```sql
-- CORRECT
ORDER BY g.game_date DESC LIMIT 10

-- WRONG: game_id may not be chronological
ORDER BY pb.game_id DESC LIMIT 10
```

---

## RULE 7 — season_year Arithmetic: Cast to Integer First

`season_year` is stored as varchar/text. Any arithmetic (subtract, compare with integer) requires an explicit `::integer` cast.

```sql
-- CORRECT
WHERE g.season_year::integer >= (SELECT MAX(season_year)::integer FROM dwh_d_games) - 9
AND g.season_year::integer BETWEEN 2015 AND 2024

-- WRONG: type error — cannot subtract integer from text
WHERE g.season_year >= MAX(season_year) - 9
WHERE g.season_year - 1 = '2023'
```

---

## RULE 8 — Back-to-Back Date Detection: LAG on Full Schedule

Use LAG on `game_date` across the full unfiltered game sequence for back-to-back detection. Date difference of 1 means back-to-back.

```sql
WITH team_schedule AS (
    SELECT g.game_id, g.game_date,
           LAG(g.game_date) OVER (ORDER BY g.game_date) AS prev_game_date
    FROM dwh_d_games g
    WHERE (g.home_team_id = :team_id OR g.visitor_team_id = :team_id)
      AND g.game_type ILIKE '%Regular Season%'
)
SELECT * FROM team_schedule
WHERE game_date - prev_game_date = 1;  -- back-to-back
```

---

## RULE 9 — Player Age at Game Date: EXTRACT(YEAR FROM AGE(...))

When calculating a player's age at the time of a game (for minutes-by-age analysis, aging curves), use `AGE(game_date, birthdate)` then EXTRACT the year component.

```sql
EXTRACT(YEAR FROM AGE(g.game_date, p.birthdate))::int AS player_age_at_game

-- Season-over-season aging curve
SELECT g.season_year,
    EXTRACT(YEAR FROM AGE(MAX(g.game_date), p.birthdate))::int AS age_that_season,
    AVG(pb.points) AS ppg,
    AVG(pb.minutes_played) AS mpg
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
WHERE p.full_name ILIKE '%LeBron James%'
  AND g.game_type ILIKE '%Regular Season%'
GROUP BY g.season_year
ORDER BY g.season_year;
```

---

## RULE 10 — Month Filter: EXTRACT or DATE_TRUNC

-- Last month (dynamic)
WHERE EXTRACT(MONTH FROM g.game_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
  AND EXTRACT(YEAR  FROM g.game_date) = EXTRACT(YEAR  FROM CURRENT_DATE - INTERVAL '1 month')

-- Named month in a specific year
WHERE DATE_TRUNC('month', g.game_date) = DATE '2025-01-01'

-- Range filter with explicit dates
WHERE g.game_date BETWEEN '2025-01-01' AND '2025-01-31'

---

## RULE 11 — Dynamic Latest Date (General)

When you need the most recent date in the data for any purpose, use MAX dynamically:

```sql
-- Most recent game overall
WHERE g.game_date = (SELECT MAX(game_date) FROM dwh_d_games)

-- Most recent game for a specific team
WHERE g.game_date = (
    SELECT MAX(g2.game_date) FROM dwh_d_games g2
    WHERE g2.home_team_id = :team_id OR g2.visitor_team_id = :team_id
)
```

---
## RULE 12 — Always Expose the Time Scope in SELECT

When a query filters by a time period (month, season, date range, last N games),
always include the resolved time scope as a column in the final SELECT.
Never return aggregates without context of what period they cover.

-- Month-scoped query: include the month
SELECT DATE_TRUNC('month', MIN(g.game_date)) AS period_month, ...

-- Season-scoped query: include the season
SELECT g.season_year, ...

-- Last N games: include the date range
SELECT MIN(g.game_date) AS from_date, MAX(g.game_date) AS to_date, ...

-- Single game (last game): include the game date
SELECT g.game_date, ...
---

## Anti-Pattern Summary

| Bad Pattern | Consequence | Fix |
|---|---|---|
| `DATE_TRUNC('year', game_date)` for seasons | Splits NBA season in two | Use `g.season_year` |
| Hardcoded `'2024'` as season year | Breaks next season | `SELECT MAX(season_year) FROM dwh_d_games` |
| `CURRENT_DATE - 1` for last game | No game = empty result | `ORDER BY game_date DESC LIMIT 1` |
| `ORDER BY game_id DESC` for recency | game_id not chronological | `ORDER BY game_date DESC` |
| `season_year - 1` without cast | Type error on text | `season_year::integer - 1` |
| Current season filter for past rookies | Player not found | `MIN(season_year)` from player_team_seasons |
| Previous season filter without `AND game_type` | Includes preseason | Add `game_type ILIKE '%Regular Season%'` |
| Aggregates with no time column in SELECT | User can't tell what period the data covers | Add DATE_TRUNC('month', ...) or season_year or date range to SELECT |
