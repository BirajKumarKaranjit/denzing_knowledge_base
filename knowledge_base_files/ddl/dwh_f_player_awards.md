---
name: dwh_f_player_awards
description: "Use when the query involves retrieving information about NBA player awards, such as MVPs, All-NBA selections, or Player of the Month honors. This table is essential for analyzing player achievements across different seasons and conferences. It includes details about the team the player was with when they received the award, the specific award description, and the time frame of the award. This is crucial for historical performance analysis, comparing player accolades, or tracking award trends over time."
tags: [player awards, NBA honors, player achievements]
priority: high
---

# DDL

```sql
CREATE TABLE dwh_f_player_awards (
    player_id text,
    team_name text,
    description text,
    all_nba_team_number text,
    season text,
    award_month date,
    award_week date,
    conference text
);
```

## Column Semantics

- **player_id**: The unique identifier for each player. This is typically used in JOIN operations with player dimension tables to gather more detailed player information such as name, position, or career statistics.
  
- **team_name**: The name of the team the player was part of when receiving the award. This can be used in SELECT or WHERE clauses to filter awards by team, especially useful for franchise-specific award analysis.

- **description**: A textual description of the award received, such as "MVP", "Defensive Player of the Year", or "Rookie of the Month". This column is often used in WHERE clauses to filter specific types of awards.

- **all_nba_team_number**: Indicates the All-NBA team level (e.g., "First Team", "Second Team"). This is particularly useful for queries focusing on All-NBA selections and is typically used in SELECT or WHERE clauses.

- **season**: Represents the NBA season in which the award was given, formatted as a string (e.g., "2022-2023"). This is crucial for temporal analysis and is often used in WHERE or GROUP BY clauses.

- **award_month**: The specific month when the award was given, useful for monthly award analysis like Player of the Month. It is typically used in WHERE clauses to filter by month.

- **award_week**: The specific week when the award was given, useful for weekly award analysis like Player of the Week. This column is often used in WHERE clauses to filter by week.

- **conference**: Indicates the conference (Eastern or Western) in which the player received the award. This can be used in SELECT or WHERE clauses to filter awards by conference.

## Common Query Patterns

- Retrieve all awards for a specific player across all seasons:
  ```sql
  SELECT * FROM dwh_f_player_awards WHERE player_id = 'player123';
  ```

- Find all MVP awards given in a specific season:
  ```sql
  SELECT * FROM dwh_f_player_awards WHERE season = '2022-2023' AND description = 'MVP';
  ```

- List all players who received Player of the Month awards in a given month:
  ```sql
  SELECT player_id, team_name FROM dwh_f_player_awards WHERE award_month = '2023-01-01';
  ```

- Count the number of All-NBA First Team selections by conference:
  ```sql
  SELECT conference, COUNT(*) FROM dwh_f_player_awards WHERE all_nba_team_number = 'First Team' GROUP BY conference;
  ```

## Join Relationships

- **player_id**: Typically joins with a player dimension table (e.g., `dwh_d_players`) to access additional player details like name, position, or career statistics.
  
- **team_name**: Can be joined with a team dimension table (e.g., `dwh_d_teams`) to gather more information about the team, such as location or team history.

- **season**: May be used to join with a season dimension table to align with other seasonal data, such as standings or team performance metrics.