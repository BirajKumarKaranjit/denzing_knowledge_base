---
name: dwh_f_player_awards
description: "Use when the query involves analyzing player awards in the context of basketball analytics, tracking player achievements across seasons, or evaluating team performance based on player accolades. This table captures detailed information about individual player awards, including the specific NBA team recognition, the season of the award, and the time frame within which the award was given. It is essential for queries that require filtering by player, team, or conference, and for aggregating awards data over time."
tags: [player awards, nba, basketball analytics, team performance]
priority: high
fk_to:
  - column: player_id
    ref_table: dwh_d_players
    ref_column: player_id
related_tables: [dwh_d_players]
---

# DDL

```sql
CREATE TABLE dwh_f_player_awards (player_id text, team_name text, description text, all_nba_team_number text, season text, award_month date, award_week date, conference text);
```

## Column Semantics

- **player_id**: A unique identifier for each player, typically used in WHERE clauses to filter data for specific players. This is a text field that may be joined with player dimension tables.
- **team_name**: The name of the team the player was part of when receiving the award. Useful in SELECT and GROUP BY clauses to analyze awards by team. Example values include "Lakers", "Celtics".
- **description**: A textual description of the award, such as "MVP", "Rookie of the Year". This field is often used in SELECT statements to display award details.
- **all_nba_team_number**: Indicates the specific All-NBA team the player was selected for, such as "First Team", "Second Team". This is a text field and can be used in WHERE clauses to filter specific team selections.
- **season**: Represents the NBA season in which the award was given, formatted as a text field like "2022-2023". Commonly used in WHERE and GROUP BY clauses for temporal analysis.
- **award_month**: The month when the award was given, stored as a date. Useful for more granular time-based analysis, often used in WHERE clauses.
- **award_week**: The week when the award was given, also stored as a date. This allows for detailed time-based filtering and analysis.
- **conference**: The conference (e.g., "Eastern", "Western") in which the player’s team competes. This is often used in WHERE and GROUP BY clauses to segment data by conference.

## Common Query Patterns

- Retrieve all awards for a specific player in a given season: `WHERE player_id = 'player123' AND season = '2022-2023'`
- Aggregate the number of awards by team and season: `GROUP BY team_name, season`
- Filter awards by conference and specific award type: `WHERE conference = 'Western' AND description = 'MVP'`
- Analyze award trends over months or weeks: `GROUP BY award_month` or `GROUP BY award_week`

## Join Relationships

- **player_id**: Typically joins with a player dimension table to enrich data with player details such as name and position.
- **team_name**: Can be joined with a team dimension table to access additional team information like location and coach.
- **conference**: May be used to join with a conference dimension table to obtain further details about the conference structure.