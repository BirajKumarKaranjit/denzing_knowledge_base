---
name: dwh_f_player_tracking
description: "Use when the query involves analyzing player movement and performance metrics during NBA games. This table provides detailed player tracking data, including speed, distance covered, and various types of assists and shot attempts. It is essential for evaluating player efficiency, defensive impact, and offensive contributions. Ideal for queries focusing on player positioning, shot contesting, and passing effectiveness."
tags: [player tracking, performance metrics, NBA analytics]
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

- **id**: Unique identifier for each record. Typically used in SELECT or WHERE clauses for specific record retrieval.
- **game_id**: Identifier for the game. Used to filter data for specific games, often in WHERE clauses.
- **team_id**: Identifier for the team. Useful for grouping or filtering data by team.
- **player_id**: Identifier for the player. Essential for player-specific queries, often used in WHERE or JOIN conditions.
- **position**: Player's position on the court (e.g., Guard, Forward). Useful for analyzing performance by position.
- **speed**: Average speed of the player during the game, measured in miles per hour. Typical values range from 0 to 20 mph.
- **distance**: Total distance covered by the player during the game, measured in miles. Commonly used in SELECT to evaluate player activity.
- **rebound_chances_offensive**: Number of opportunities a player had to grab an offensive rebound. Used in SELECT or GROUP BY to assess rebounding effectiveness.
- **rebound_chances_defensive**: Number of opportunities a player had to grab a defensive rebound.
- **touches**: Number of times a player touched the ball. Indicates involvement in the game, often used in SELECT or GROUP BY.
- **secondary_assists**: Passes leading to an assist, also known as hockey assists. Used to measure playmaking ability.
- **free_throw_assists**: Passes leading to free throw opportunities. Useful for evaluating passing effectiveness.
- **passes**: Total number of passes made by the player. Indicates passing activity, often used in SELECT.
- **contested_field_goals_made**: Number of field goals made while being contested by a defender.
- **contested_field_goals_attempted**: Number of field goals attempted while being contested.
- **uncontested_field_goals_made**: Number of field goals made without defensive pressure.
- **uncontested_field_goals_attempted**: Number of field goals attempted without defensive pressure.
- **defended_at_rim_field_goals_made**: Field goals made when defended at the rim.
- **defended_at_rim_field_goals_attempted**: Field goals attempted when defended at the rim.

## Common Query Patterns

- Retrieve player performance metrics for a specific game: `SELECT * FROM dwh_f_player_tracking WHERE game_id = '20231010';`
- Analyze team performance by aggregating player data: `SELECT team_id, AVG(speed), SUM(distance) FROM dwh_f_player_tracking GROUP BY team_id;`
- Evaluate player efficiency in contested vs. uncontested shots: `SELECT player_id, contested_field_goals_made, uncontested_field_goals_made FROM dwh_f_player_tracking WHERE player_id = '12345';`
- Compare defensive impact by analyzing defended at rim statistics: `SELECT player_id, defended_at_rim_field_goals_made, defended_at_rim_field_goals_attempted FROM dwh_f_player_tracking;`

## Join Relationships

- **game_id**: Typically joins with a games table to get additional game details.
- **team_id**: Joins with a teams table to retrieve team information.
- **player_id**: Joins with a players table to get player demographics and career stats.