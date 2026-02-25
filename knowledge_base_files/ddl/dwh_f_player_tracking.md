---
name: dwh_f_player_tracking
description: "Use when the query involves detailed player performance metrics during games, such as tracking player speed, distance covered, and various shooting statistics. This table is essential for analyzing player efficiency, defensive capabilities, and offensive contributions in basketball games. It provides granular data on player movements and actions, which can be used to evaluate performance trends, compare players, and optimize team strategies. Ideal for queries focused on player tracking analytics, game performance analysis, and sports data insights."
tags: [player tracking, performance metrics, basketball analytics]
priority: high
---

# DDL

```sql
CREATE TABLE dwh_f_player_tracking (
    id text,
    game_id text,
    team_id text,
    player_id text,
    position text,
    speed numeric,
    distance numeric,
    rebound_chances_offensive numeric,
    rebound_chances_defensive numeric,
    touches numeric,
    secondary_assists numeric,
    free_throw_assists numeric,
    passes numeric,
    contested_field_goals_made numeric,
    contested_field_goals_attempted numeric,
    uncontested_field_goals_made numeric,
    uncontested_field_goals_attempted numeric,
    defended_at_rim_field_goals_made numeric,
    defended_at_rim_field_goals_attempted numeric
);
```

## Column Semantics

- **id**: Unique identifier for each record. Typically used in SELECT and WHERE clauses for specific record retrieval.
- **game_id**: Identifier for the game. Used to filter or group data by specific games.
- **team_id**: Identifier for the team. Useful for aggregating or filtering data by team.
- **player_id**: Identifier for the player. Central to queries focusing on individual player performance.
- **position**: Player's position during the game (e.g., guard, forward). Useful for filtering or grouping by position.
- **speed**: Numeric value representing the player's speed, typically in meters per second. Used in performance analysis.
- **distance**: Total distance covered by the player during the game, usually in meters. Important for assessing player activity levels.
- **rebound_chances_offensive**: Number of opportunities the player had to secure an offensive rebound. Used in performance metrics.
- **rebound_chances_defensive**: Number of opportunities for defensive rebounds. Important for defensive performance analysis.
- **touches**: Number of times the player handled the ball. Key for understanding player involvement.
- **secondary_assists**: Assists that lead to a scoring opportunity after an additional pass. Used in advanced playmaking analysis.
- **free_throw_assists**: Assists leading to free throw opportunities. Useful for evaluating playmaking efficiency.
- **passes**: Total number of passes made by the player. Important for understanding player involvement in ball movement.
- **contested_field_goals_made**: Number of field goals made under defensive pressure. Used to assess shooting efficiency under pressure.
- **contested_field_goals_attempted**: Total contested shots attempted. Important for evaluating shooting decisions.
- **uncontested_field_goals_made**: Field goals made without defensive pressure. Used to assess shooting efficiency.
- **uncontested_field_goals_attempted**: Total uncontested shots attempted. Important for evaluating shooting opportunities.
- **defended_at_rim_field_goals_made**: Successful shots made while being defended at the rim. Used in defensive performance analysis.
- **defended_at_rim_field_goals_attempted**: Total attempts while being defended at the rim. Important for evaluating defensive pressure.

## Common Query Patterns

- Retrieve player performance metrics for a specific game: `SELECT * FROM dwh_f_player_tracking WHERE game_id = 'game123';`
- Analyze team performance by aggregating player data: `SELECT team_id, SUM(points) FROM dwh_f_player_tracking GROUP BY team_id;`
- Compare player efficiency in contested vs. uncontested shots: `SELECT player_id, contested_field_goals_made, uncontested_field_goals_made FROM dwh_f_player_tracking WHERE player_id = 'player456';`
- Evaluate defensive capabilities by analyzing defended shots: `SELECT player_id, defended_at_rim_field_goals_made FROM dwh_f_player_tracking WHERE defended_at_rim_field_goals_attempted > 10;`

## Join Relationships

- **game_id**: Typically joins with a games table to provide context about the game (e.g., date, location).
- **team_id**: Joins with a teams table to retrieve team details such as name and league.
- **player_id**: Joins with a players table to access player demographics and historical performance data.