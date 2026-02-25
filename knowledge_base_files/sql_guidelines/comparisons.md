---
name: comparisons
description: "Use when the query involves complex comparisons such as CASE WHEN expressions, subquery comparisons against aggregated values, side-by-side entity comparisons, HAVING threshold filters, and conditional aggregations using FILTER (WHERE ...). This guideline is particularly useful for scenarios where you need to derive insights from player and team performance metrics, compare game outcomes, or filter data based on specific conditions. It helps in constructing queries that require nuanced logic and precise filtering to extract meaningful analytics from the dataset."
tags: [SQL, comparisons, CASE WHEN, subqueries, HAVING, FILTER]
priority: high
---

# SQL Comparisons Guide

This guide provides practical SQL patterns and templates for performing various types of comparisons using the tables and columns defined in the provided DDL. These patterns are essential for extracting insights from complex datasets, particularly in sports analytics.

## CASE WHEN Expressions

Use `CASE WHEN` expressions to create conditional logic within your queries. This is useful for categorizing data or creating new calculated fields based on existing data.

```sql
SELECT
    player_id,
    full_name,
    CASE 
        WHEN player_height > 200 THEN 'Tall'
        WHEN player_height BETWEEN 180 AND 200 THEN 'Average'
        ELSE 'Short'
    END AS height_category
FROM dwh_d_players;
```

### Gotchas
- Ensure that all possible conditions are covered to avoid unexpected NULL results.
- Be mindful of the order of conditions; they are evaluated sequentially.

## Subquery Comparisons Against Aggregated Values

Subqueries can be used to compare individual records against aggregated values, such as averages or totals.

```sql
SELECT
    game_id,
    home_team_id,
    visitor_team_id,
    home_score,
    visitor_score
FROM dwh_d_games
WHERE home_score > (
    SELECT AVG(home_score) FROM dwh_d_games
);
```

### Gotchas
- Subqueries can be performance-intensive; consider indexing columns involved in subqueries.
- Ensure subqueries return a single value when used in comparison operations.

## Side-by-Side Entity Comparisons

Perform side-by-side comparisons to evaluate differences or similarities between entities, such as teams or players.

```sql
SELECT
    t1.team_id AS team_id_1,
    t1.full_name AS team_name_1,
    t2.team_id AS team_id_2,
    t2.full_name AS team_name_2
FROM dwh_d_teams t1
JOIN dwh_d_teams t2 ON t1.conference = t2.conference
WHERE t1.team_id <> t2.team_id;
```

### Gotchas
- Ensure that the join conditions are correctly set to avoid Cartesian products.
- Use aliases to differentiate between columns from different instances of the same table.

## HAVING Threshold Filters

Use the `HAVING` clause to filter groups based on aggregated values.

```sql
SELECT
    team_id,
    COUNT(game_id) AS games_played
FROM dwh_f_player_team_seasons
GROUP BY team_id
HAVING COUNT(game_id) > 50;
```

### Gotchas
- The `HAVING` clause is used after `GROUP BY` and is applied to aggregated results.
- Avoid using `HAVING` for conditions that can be placed in the `WHERE` clause for better performance.

## Conditional Aggregations Using FILTER (WHERE ...)

The `FILTER` clause allows for conditional aggregation, which is useful for calculating metrics under specific conditions.

```sql
SELECT
    player_id,
    SUM(points) FILTER (WHERE game_type = 'Regular') AS regular_season_points,
    SUM(points) FILTER (WHERE game_type = 'Playoff') AS playoff_points
FROM dwh_f_player_boxscore
GROUP BY player_id;
```

### Gotchas
- Ensure that the conditions in the `FILTER` clause are correctly specified to avoid incorrect aggregations.
- The `FILTER` clause is only available in PostgreSQL 9.4 and later.

## Multi-Table Query Example

Combining multiple tables to derive insights can be powerful. Here is an example that combines player and game data to calculate average points per game for each player.

```sql
SELECT
    p.player_id,
    p.full_name,
    AVG(b.points) AS avg_points_per_game
FROM dwh_d_players p
JOIN dwh_f_player_boxscore b ON p.player_id = b.player_id
GROUP BY p.player_id, p.full_name
HAVING AVG(b.points) > 10;
```

### Gotchas
- Ensure that join conditions are correctly specified to avoid incorrect data merging.
- Use `HAVING` to filter aggregated results, not individual records.