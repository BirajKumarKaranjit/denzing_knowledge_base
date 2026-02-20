---
name: dwh_f_player_awards
description: "Use when the query involves retrieving information about NBA player awards, such as MVPs, All-NBA Team selections, or Player of the Month honors. This table is essential for analyzing player achievements across different seasons and conferences. It includes details about the team the player was on when receiving the award, the specific award description, and the timing of the award. Ideal for queries that need to track player accolades over time or compare award distributions across conferences."
tags: [player awards, NBA achievements, player accolades]
priority: medium
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

- **player_id**: Unique identifier for each player. Used to join with player dimension tables. Typically used in SELECT and WHERE clauses to filter or display specific player data.
- **team_name**: The name of the team the player was part of when receiving the award. Useful for historical context and team-based queries. Commonly used in SELECT statements.
- **description**: Textual description of the award (e.g., "MVP", "Rookie of the Year"). This column is crucial for filtering specific types of awards in WHERE clauses.
- **all_nba_team_number**: Indicates the All-NBA Team number (e.g., "1st", "2nd", "3rd"). Relevant for queries focusing on All-NBA selections. Often used in SELECT and WHERE clauses.
- **season**: The NBA season during which the award was given, formatted as a string (e.g., "2022-2023"). Essential for time-based analysis and typically used in WHERE and GROUP BY clauses.
- **award_month**: The month when the award was given, useful for monthly awards like Player of the Month. Used in SELECT and WHERE clauses for time-specific queries.
- **award_week**: The week when the award was given, relevant for weekly awards. Similar usage to award_month.
- **conference**: The conference (Eastern or Western) associated with the award. Important for regional analysis and often used in WHERE clauses.

## Common Query Patterns

- Retrieve all awards for a specific player across multiple seasons: `SELECT * FROM dwh_f_player_awards WHERE player_id = '12345';`
- Analyze the distribution of MVP awards by conference: `SELECT conference, COUNT(*) FROM dwh_f_player_awards WHERE description = 'MVP' GROUP BY conference;`
- List all players who made the All-NBA 1st Team in a given season: `SELECT player_id FROM dwh_f_player_awards WHERE all_nba_team_number = '1st' AND season = '2022-2023';`

## Join Relationships

- **player_id**: Typically joins with a player dimension table (e.g., `dwh_d_players`) to get additional player details like name and position.
- **team_name**: Can be joined with a team dimension table (e.g., `dwh_d_teams`) to retrieve more information about the team, such as location and team history.
- **season**: Often used to join with a season dimension table to align with other seasonal data, such as team performance or league statistics.