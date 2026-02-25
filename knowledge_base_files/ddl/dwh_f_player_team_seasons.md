---
name: dwh_f_player_team_seasons
description: "Use when the query involves analyzing player performance across different seasons and teams. This table is crucial for understanding how many games a player participated in during a specific season, under various game types. It is often used in conjunction with player and team analytics to evaluate performance trends over time. Queries may focus on aggregating player participation data, filtering by specific seasons or teams, and comparing game types."
tags: [player, team, season, games, analytics]
priority: high
fk_to:
  - column: player_id
    ref_table: dwh_d_players
    ref_column: player_id
  - column: team_id
    ref_table: dwh_d_teams
    ref_column: team_id
related_tables: [dwh_d_players, dwh_d_teams]
---

# DDL

```sql
CREATE TABLE dwh_f_player_team_seasons (player_id text, season text, team_id text, game_type text, games_played numeric);
```

## Column Semantics

- **player_id**: Represents the unique identifier for each player. Typically used in WHERE clauses to filter data for specific players or in JOINs with player dimension tables. Values are alphanumeric strings.
  
- **season**: Indicates the season during which the games were played. Commonly used in WHERE and GROUP BY clauses to segment data by time periods. Values are usually in a 'YYYY-YYYY' format, such as '2022-2023'.

- **team_id**: Denotes the unique identifier for each team. Often used in WHERE clauses to filter data for specific teams or in JOINs with team dimension tables. Values are alphanumeric strings.

- **game_type**: Specifies the type of game, such as 'regular', 'playoff', etc. Useful for filtering and grouping data by game type. Values are typically categorical strings.

- **games_played**: Represents the number of games a player has participated in during the specified season and game type. This numeric field is often aggregated in SELECT statements to calculate totals or averages.

## Common Query Patterns

- Retrieve the total number of games played by a specific player in a given season:
  ```sql
  SELECT SUM(games_played) FROM dwh_f_player_team_seasons WHERE player_id = 'P123' AND season = '2022-2023';
  ```

- Compare the number of games played by players across different teams in a specific season:
  ```sql
  SELECT team_id, SUM(games_played) FROM dwh_f_player_team_seasons WHERE season = '2022-2023' GROUP BY team_id;
  ```

- Filter games played by game type for a particular player:
  ```sql
  SELECT game_type, games_played FROM dwh_f_player_team_seasons WHERE player_id = 'P123' AND season = '2022-2023';
  ```

## Join Relationships

- **player_id**: Typically joins with a player dimension table on player_id to enrich data with player details such as name, position, etc.

- **team_id**: Commonly joins with a team dimension table on team_id to access team-specific information like team name, location, etc.

- **season**: May be used in conjunction with a calendar or season dimension table to provide additional temporal context or attributes related to the season.