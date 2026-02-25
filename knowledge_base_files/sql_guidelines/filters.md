---
name: filters
description: "Use when the query involves filtering data based on text lookup, categorical values, numeric ranges, status flags, or handling NULL values in a Postgres database. This guideline is essential for crafting efficient WHERE clauses, especially when dealing with case-insensitive text searches using ILIKE, filtering with IN/ANY for multiple values, and ensuring correct handling of NULLs. It is particularly useful for queries that need to filter player statistics, game details, or team information based on specific criteria."
tags: [filters, WHERE clause, text lookup, numeric range, NULL handling]
priority: high
---

# SQL Filtering Guidelines

## Text Lookup with ILIKE

When performing case-insensitive text searches, use the `ILIKE` operator. This is particularly useful for searching player names or team nicknames.

```sql
SELECT player_id, full_name
FROM dwh_d_players
WHERE full_name ILIKE '%john%';
```

**Gotcha:** Avoid using `ILIKE` with leading wildcards (`%`) as it can lead to full table scans, which are inefficient.

## Categorical Filtering with IN

Use the `IN` operator to filter rows based on a list of categorical values. This is effective for filtering games by type or players by position.

```sql
SELECT game_id, game_date, game_type
FROM dwh_d_games
WHERE game_type IN ('Regular Season', 'Playoffs');
```

**Anti-pattern:** Using `OR` for multiple categorical conditions can be less readable and harder to maintain than `IN`.

## Numeric Range Filters

For filtering based on numeric ranges, use comparison operators. This is useful for filtering player statistics or game scores.

```sql
SELECT player_id, points
FROM dwh_f_player_boxscore
WHERE points BETWEEN 20 AND 30;
```

**Gotcha:** Ensure that the column used in the range filter is indexed for better performance.

## Status/Flag Filtering

Filter data based on status or flag columns to retrieve active or specific flagged records.

```sql
SELECT team_id, full_name
FROM dwh_d_teams
WHERE active_status = 'Active';
```

**Anti-pattern:** Avoid using non-boolean columns as flags without clear documentation, as it can lead to confusion.

## NULL Handling

Use `IS NULL` or `IS NOT NULL` to handle NULL values in your filters. This is crucial for columns that may have missing data.

```sql
SELECT player_id, school
FROM dwh_d_players
WHERE school IS NOT NULL;
```

**Gotcha:** Remember that `NULL` is not equal to anything, including another `NULL`. Use `IS NULL` instead of `=` for comparisons.

## Multi-Table Query Example

Combine filters across multiple tables to extract comprehensive insights. This example retrieves player statistics for a specific team and season.

```sql
SELECT p.full_name, pb.points, pb.assists, pb.rebounds_offensive
FROM dwh_d_players p
JOIN dwh_f_player_boxscore pb ON p.player_id = pb.player_id
JOIN dwh_f_player_team_seasons pts ON p.player_id = pts.player_id
WHERE pts.team_id = 'LAL'
  AND pts.season = '2022'
  AND pb.points > 15;
```

**Gotcha:** Ensure that join conditions are correctly specified to avoid Cartesian products, which can lead to performance issues.