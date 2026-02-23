---
name: dwh_f_player_tracking
description: "Use when the query involves analyzing player movement and performance metrics during NBA games. This table provides detailed tracking data such as player speed, distance covered, and various assist and scoring metrics. It's essential for evaluating player efficiency, defensive capabilities, and offensive contributions. Ideal for queries that require insights into player positioning, shot contesting, and passing effectiveness."
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

- **id**: Unique identifier for each record; typically used in SELECT or WHERE clauses.
- **game_id**: Identifier for the game; used to filter data for specific games or join with game-related tables.
- **team_id**: Identifier for the team; useful for team-based analysis or joins with team tables.
- **player_id**: Identifier for the player; crucial for player-specific queries and joins with player dimension tables.
- **position**: Player's position on the court (e.g., guard, forward); used in SELECT or GROUP BY for positional analysis.
- **speed**: Average speed of the player during the game, measured in miles per hour; used to assess player activity levels.
- **distance**: Total distance covered by the player, typically in miles; indicates player involvement and stamina.
- **rebound_chances_offensive**: Opportunities for offensive rebounds; higher values suggest active presence near the basket.
- **rebound_chances_defensive**: Opportunities for defensive rebounds; used to evaluate defensive positioning.
- **touches**: Number of times the player handled the ball; indicates involvement in offensive plays.
- **secondary_assists**: Passes leading to an assist; measures contribution to team play beyond direct assists.
- **free_throw_assists**: Passes leading to free throw opportunities; reflects playmaking ability.
- **passes**: Total number of passes made; used to assess passing frequency and team play style.
- **contested_field_goals_made**: Successful field goals made under defensive pressure; indicates scoring ability under duress.
- **contested_field_goals_attempted**: Field goals attempted under defensive pressure; used to evaluate shot selection.
- **uncontested_field_goals_made**: Successful field goals made with no defensive pressure; reflects ability to capitalize on open shots.
- **uncontested_field_goals_attempted**: Field goals attempted with no defensive pressure; used to assess shot opportunities.
- **defended_at_rim_field_goals_made**: Successful field goals made at the rim with defensive pressure; measures finishing ability.
- **defended_at_rim_field_goals_attempted**: Field goals attempted at the rim with defensive pressure; used to evaluate rim attack efficiency.

## Common Query Patterns

- Analyze player performance by filtering on `game_id` and `player_id` to get specific game data.
- Calculate average speed and distance for players in a season using `GROUP BY player_id`.
- Assess team passing dynamics by aggregating `passes` and `secondary_assists` per `team_id`.
- Evaluate defensive effectiveness by comparing `contested_field_goals_made` and `defended_at_rim_field_goals_made`.

## Join Relationships

- Join with `dwh_dim_players` on `player_id` to get player demographics and historical data.
- Join with `dwh_dim_teams` on `team_id` to access team information and statistics.
- Join with `dwh_dim_games` on `game_id` for game-specific details and context.