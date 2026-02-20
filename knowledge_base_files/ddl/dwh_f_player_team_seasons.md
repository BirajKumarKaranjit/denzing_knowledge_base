---
name: dwh_f_player_team_seasons
description: "Use when the query involves analyzing player participation across different NBA seasons and teams. This table is essential for understanding a player's career trajectory, team affiliations, and game participation over time. It is particularly useful for queries that require filtering by specific seasons, teams, or game types (e.g., regular season, playoffs). Analysts can leverage this table to calculate metrics like average games played per season or to track player movement between teams."
tags: [player, team, season, games, nba]
priority: high
---

# DDL

```sql
CREATE TABLE dwh_f_player_team_seasons (player_id text, season text, team_id text, game_type text, games_played numeric);
```

## Column Semantics

- **player_id**: Represents the unique identifier for each player. This is crucial for joining with player dimension tables to fetch player details like name, position, and statistics. Typically used in WHERE and JOIN clauses.
  
- **season**: Indicates the NBA season, formatted as a string (e.g., '2022-2023'). This column is essential for filtering data by specific seasons and is often used in WHERE and GROUP BY clauses.

- **team_id**: Represents the unique identifier for each team. This is used to join with team dimension tables to get team names and other attributes. Commonly used in WHERE and JOIN clauses to filter or aggregate data by team.

- **game_type**: Specifies the type of games, such as 'regular' or 'playoffs'. This allows analysts to differentiate between regular season and playoff performance. Used in WHERE clauses to filter data by game type.

- **games_played**: A numeric value indicating the number of games a player participated in during a specific season for a team. This is used in SELECT statements to calculate averages or totals and can be aggregated in GROUP BY clauses. Values typically range from 0 to 82 for regular seasons, with higher values possible when including playoffs.

## Common Query Patterns

- Retrieve the total number of games played by a player across all teams in a specific season:
  ```sql
  SELECT SUM(games_played) FROM dwh_f_player_team_seasons WHERE player_id = 'player123' AND season = '2022-2023';
  ```

- Find players who played in the playoffs for a specific team:
  ```sql
  SELECT player_id FROM dwh_f_player_team_seasons WHERE team_id = 'team456' AND game_type = 'playoffs';
  ```

- Compare the number of games played by a player in regular seasons versus playoffs:
  ```sql
  SELECT game_type, SUM(games_played) FROM dwh_f_player_team_seasons WHERE player_id = 'player123' GROUP BY game_type;
  ```

## Join Relationships

- **player_id**: Typically joins with a player dimension table to access player-specific details such as name, position, and career statistics.

- **team_id**: Joins with a team dimension table to retrieve team names, locations, and other team-related information.

- **season**: Can be used to join with a calendar or season dimension table to get additional context about the season, such as start and end dates.