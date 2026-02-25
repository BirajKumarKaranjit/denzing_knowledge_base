---
name: performance
description: "Use when the query involves optimizing SQL performance by filtering data before joining tables, indexing primary and foreign keys, using LIMIT for large result sets, choosing between EXISTS and IN for subqueries, and avoiding expensive full-table scans by pushing predicates early. This guidance is crucial for improving query efficiency in analytics databases, especially when dealing with large datasets and complex joins."
tags: [performance, optimization, indexing, filtering]
priority: high
---

## Filter-Before-Join Optimization

When joining large tables, always apply filters before the join to reduce the dataset size and improve performance.

```sql
SELECT g.game_id, g.game_date, t.full_name
FROM dwh_d_games g
JOIN dwh_d_teams t ON g.home_team_id = t.team_id
WHERE g.season_year = '2023'
  AND t.conference = 'East';
```

**Gotcha:** Avoid applying filters after the join, as this can lead to unnecessary data processing and slower queries.

## Indexing Strategy

Index columns that are frequently used in WHERE clauses, JOIN conditions, and as foreign keys to speed up data retrieval.

- **Primary Keys**: Typically indexed by default.
- **Foreign Keys**: Consider indexing `home_team_id`, `visitor_team_id` in `dwh_d_games`, and `player_id` in `dwh_f_player_boxscore`.

**Example:**

```sql
-- Create an index on the foreign key column
CREATE INDEX idx_home_team_id ON dwh_d_games(home_team_id);
```

**Gotcha:** Over-indexing can lead to increased storage and maintenance overhead. Index only where necessary.

## Using LIMIT for Large Result Sets

When querying large tables, use the `LIMIT` clause to restrict the number of rows returned, which can significantly reduce query execution time.

```sql
SELECT player_id, full_name, position
FROM dwh_d_players
WHERE country = 'USA'
LIMIT 100;
```

**Gotcha:** Be cautious with LIMIT in pagination; combine with ORDER BY to ensure consistent results.

## EXISTS vs IN for Subqueries

Use `EXISTS` for subqueries when checking for the existence of rows, as it can be more efficient than `IN` with large datasets.

```sql
-- Using EXISTS
SELECT p.player_id, p.full_name
FROM dwh_d_players p
WHERE EXISTS (
  SELECT 1
  FROM dwh_f_player_awards a
  WHERE a.player_id = p.player_id
    AND a.season = '2023'
);
```

**Gotcha:** `IN` can be less efficient with subqueries returning a large number of rows.

## Avoiding Full-Table Scans

Push predicates early in the query to avoid full-table scans, which are costly in terms of performance.

```sql
SELECT g.game_id, g.game_date, t.full_name
FROM dwh_d_games g
JOIN dwh_d_teams t ON g.home_team_id = t.team_id
WHERE g.game_date BETWEEN '2023-01-01' AND '2023-12-31'
  AND t.active_status = 'Active';
```

**Gotcha:** Ensure that predicates are applied before joins to minimize the data processed.

## Complete Multi-Table Query Example

Here's a comprehensive example that combines several performance optimization techniques:

```sql
SELECT g.game_id, g.game_date, ht.full_name AS home_team, vt.full_name AS visitor_team
FROM dwh_d_games g
JOIN dwh_d_teams ht ON g.home_team_id = ht.team_id
JOIN dwh_d_teams vt ON g.visitor_team_id = vt.team_id
WHERE g.season_year = '2023'
  AND ht.conference = 'East'
  AND vt.conference = 'West'
  AND EXISTS (
    SELECT 1
    FROM dwh_f_team_championships c
    WHERE c.team_id = ht.team_id
      AND c.yearawarded = '2023'
  )
LIMIT 50;
```

This query efficiently retrieves game details by filtering on season year and team conferences before joining, using EXISTS for subquery optimization, and limiting the result set.