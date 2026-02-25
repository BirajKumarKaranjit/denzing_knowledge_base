---
name: aggregations
description: "Use when the query involves calculating totals, averages, or counts across dimensions such as games, players, or teams. This includes scenarios where you need to group data by specific columns, apply aggregate functions like SUM, AVG, or COUNT, and filter results using HAVING clauses. Additionally, this category covers the use of window functions for ranking or calculating running totals, and techniques like NULLIF to safely handle division operations that might result in divide-by-zero errors."
tags: [aggregations, group by, window functions, safe division]
priority: high
---

## GROUP BY Patterns

When aggregating data, the `GROUP BY` clause is essential for organizing results by specific dimensions. Here's how you can calculate the total points scored by each team in each game:

```sql
SELECT 
    game_id,
    team_id,
    SUM(points) AS total_points
FROM 
    dwh_f_team_boxscore
GROUP BY 
    game_id, team_id;
```

### Gotcha: Missing GROUP BY Columns

Ensure that all non-aggregated columns in the `SELECT` clause are included in the `GROUP BY` clause to avoid SQL errors.

## Using Aggregate Functions

Aggregate functions like `SUM`, `AVG`, and `COUNT` are commonly used in analytics queries. Here's an example of calculating the average player height by team:

```sql
SELECT 
    team_id,
    AVG(player_height) AS average_height
FROM 
    dwh_d_players
GROUP BY 
    team_id;
```

### Anti-pattern: Aggregating Without GROUP BY

Avoid using aggregate functions without a `GROUP BY` clause unless you intend to aggregate over the entire dataset.

## HAVING Clauses

The `HAVING` clause is used to filter results after aggregation. For example, to find teams with an average player weight over 200:

```sql
SELECT 
    team_id,
    AVG(player_weight) AS average_weight
FROM 
    dwh_d_players
GROUP BY 
    team_id
HAVING 
    AVG(player_weight) > 200;
```

### Gotcha: HAVING vs WHERE

Remember that `HAVING` is used for conditions on aggregated data, while `WHERE` is used for row-level filtering before aggregation.

## Window Functions

Window functions allow calculations across a set of table rows related to the current row. Here's how to rank players by points scored in each game:

```sql
SELECT 
    player_id,
    game_id,
    points,
    RANK() OVER (PARTITION BY game_id ORDER BY points DESC) AS player_rank
FROM 
    dwh_f_player_boxscore;
```

### Anti-pattern: Misusing Window Functions

Avoid using window functions when a simple `GROUP BY` would suffice, as they can be more computationally expensive.

## Safe Division with NULLIF

To prevent divide-by-zero errors, use `NULLIF` in division operations. Here's an example calculating the field goal percentage:

```sql
SELECT 
    player_id,
    game_id,
    field_goals_made,
    field_goals_attempted,
    (field_goals_made::numeric / NULLIF(field_goals_attempted, 0)) * 100 AS field_goal_percentage
FROM 
    dwh_f_player_boxscore;
```

## Multi-Table Query Example

Combining data from multiple tables can provide richer insights. Here's how to calculate the total points scored by players from each team in a specific season:

```sql
SELECT 
    t.team_id,
    t.full_name AS team_name,
    SUM(pb.points) AS total_points
FROM 
    dwh_f_player_boxscore pb
JOIN 
    dwh_d_players p ON pb.player_id = p.player_id
JOIN 
    dwh_d_teams t ON p.team_id = t.team_id
WHERE 
    pb.game_id IN (
        SELECT game_id 
        FROM dwh_d_games 
        WHERE season_year = '2023'
    )
GROUP BY 
    t.team_id, t.full_name;
```

This query joins player box scores with player and team details, filtering for games in the 2023 season and aggregating total points by team.