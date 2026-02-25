---
name: date_handling
description: "Use when the query involves manipulating dates for period grouping, filtering by date ranges, extracting specific date components like year or month, and dynamically finding the latest available period using MAX(). This guideline is essential for avoiding hard-coded dates in production queries, ensuring flexibility and accuracy in time-based data analysis. It is particularly useful in scenarios involving game schedules, player statistics over time, and team performance metrics."
tags: [date_trunc, extract, date_range, max_date]
priority: high
---

## Grouping by Period with DATE_TRUNC

When you need to group data by specific time periods such as month or year, `DATE_TRUNC` is your friend. This function truncates a date or timestamp to the specified precision.

```sql
SELECT 
    DATE_TRUNC('month', game_date) AS month,
    COUNT(game_id) AS games_played
FROM 
    dwh_d_games
GROUP BY 
    DATE_TRUNC('month', game_date)
ORDER BY 
    month;
```

### Gotcha
- Ensure the column you are truncating is of type `date` or `timestamp`. Truncating other types will result in errors.

## Filtering by Date Range

To filter records within a specific date range, use the `BETWEEN` operator for clarity and readability.

```sql
SELECT 
    game_id, 
    game_date, 
    home_team_id, 
    visitor_team_id
FROM 
    dwh_d_games
WHERE 
    game_date BETWEEN '2023-01-01' AND '2023-12-31';
```

### Anti-pattern
- Avoid using hard-coded dates directly in your queries. Instead, use parameters or variables to make your queries more flexible and maintainable.

## Extracting Year and Month with EXTRACT

The `EXTRACT` function is useful for pulling out specific components of a date, such as the year or month.

```sql
SELECT 
    EXTRACT(YEAR FROM game_date) AS year,
    EXTRACT(MONTH FROM game_date) AS month,
    COUNT(game_id) AS games_played
FROM 
    dwh_d_games
GROUP BY 
    year, month
ORDER BY 
    year, month;
```

### Gotcha
- The `EXTRACT` function returns a double precision value, so be mindful of this when using it in calculations or comparisons.

## Finding the Latest Period with MAX()

To dynamically find the latest available date or period, use the `MAX()` function. This is particularly useful for reports that need to show the most recent data.

```sql
SELECT 
    MAX(game_date) AS latest_game_date
FROM 
    dwh_d_games;
```

### Anti-pattern
- Avoid assuming the latest date is today or a fixed date. Always calculate it dynamically to ensure accuracy.

## Multi-Table Query Example

Combining date handling with joins across multiple tables can provide comprehensive insights. Here’s how you can find the latest game for each team and their scores.

```sql
SELECT 
    g.home_team_id,
    t.full_name AS home_team_name,
    g.visitor_team_id,
    vt.full_name AS visitor_team_name,
    g.game_date,
    g.home_score,
    g.visitor_score
FROM 
    dwh_d_games g
JOIN 
    dwh_d_teams t ON g.home_team_id = t.team_id
JOIN 
    dwh_d_teams vt ON g.visitor_team_id = vt.team_id
WHERE 
    g.game_date = (SELECT MAX(game_date) FROM dwh_d_games)
ORDER BY 
    g.game_date DESC;
```

### Gotcha
- Ensure that the subquery for the latest date is correctly correlated if needed, to avoid performance issues or incorrect results.