---
name: dwh_f_player_team_seasons
description: "Use when the query involves analyzing player participation across different NBA seasons and teams. This table is essential for understanding a player's career trajectory, including which teams they played for in specific seasons and how many games they participated in. It is particularly useful for queries related to player movement, team composition over time, and game type distinctions such as regular season versus playoffs. This table helps in evaluating player consistency and team dynamics across multiple seasons."
tags: [player, team, season, games]
priority: high
---

# DDL

```sql
CREATE TABLE dwh_f_player_team_seasons (player_id text, season text, team_id text, game_type text, games_played numeric);
```

## Column Semantics

- **player_id**: Represents the unique identifier for each player. This is typically a foreign key that links to a player dimension table. Used in WHERE clauses to filter data for specific players or in JOINs to gather more player-specific information.
  
- **season**: Denotes the NBA season, usually formatted as 'YYYY-YYYY' (e.g., '2022-2023'). This column is crucial for temporal analysis and is often used in WHERE and GROUP BY clauses to segment data by season.

- **team_id**: Identifies the team for which the player played during the specified season. This is a foreign key that usually links to a team dimension table. Commonly used in JOINs to fetch team details or in WHERE clauses to filter by team.

- **game_type**: Specifies the type of games the data pertains to, such as 'Regular Season' or 'Playoffs'. This column is important for distinguishing between different competitive contexts and is often used in WHERE clauses.

- **games_played**: Indicates the number of games the player participated in during the specified season and game type. This numeric value is used in SELECT statements to calculate averages or totals and can be used in WHERE clauses to filter players based on participation.

## Common Query Patterns

- Retrieve the number of games played by a specific player in a given season: 
  ```sql
  SELECT games_played FROM dwh_f_player_team_seasons WHERE player_id = '123' AND season = '2022-2023';
  ```

- Analyze player participation across multiple seasons for a specific team:
  ```sql
  SELECT season, games_played FROM dwh_f_player_team_seasons WHERE team_id = '456' AND player_id = '123';
  ```

- Compare regular season and playoff participation for players:
  ```sql
  SELECT player_id, SUM(games_played) FROM dwh_f_player_team_seasons WHERE game_type = 'Playoffs' GROUP BY player_id;
  ```

## Join Relationships

- **player_id**: Typically joins with a player dimension table to fetch player details such as name, position, and career stats.
  
- **team_id**: Joins with a team dimension table to obtain team information like team name, location, and historical performance.

- This table is often joined with a game results table to correlate player participation with game outcomes, providing insights into player impact on team success.