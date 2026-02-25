---
name: dwh_d_games
description: "Use when the query involves analyzing game details, scores, and team matchups in a sports analytics context. This table provides comprehensive information about each game, including identifiers, season details, and scores, making it essential for performance analysis, historical data retrieval, and trend analysis over seasons. It is particularly useful for queries that require filtering by game date, team performance, or specific game types such as playoffs. The table supports detailed breakdowns of home and visitor team statistics and is crucial for generating reports on game outcomes and team comparisons."
tags: [games, sports, analytics, scores, teams]
priority: high
fk_to:
  - column: home_team_id
    ref_table: dwh_d_teams
    ref_column: team_id
  - column: visitor_team_id
    ref_table: dwh_d_teams
    ref_column: team_id
related_tables: [dwh_d_teams, dwh_f_team_boxscore, dwh_f_player_boxscore]
---

# DDL

```sql
CREATE TABLE dwh_d_games (
    game_id text,
    gamecode text,
    season_year text,
    season text,
    game_date date,
    game_time time without time zone,
    arena_name text,
    home_team_id text,
    visitor_team_id text,
    home_score numeric,
    visitor_score numeric,
    game_type text,
    playoff_round text
);
```

## Column Semantics

- **game_id**: A unique identifier for each game, typically used in SELECT and WHERE clauses to retrieve specific game data.
- **gamecode**: An alternate identifier for games, often used for integration with external systems or APIs.
- **season_year**: Represents the year of the season, useful for filtering games by season in WHERE clauses. Example values include '2022', '2023'.
- **season**: Describes the season type (e.g., 'Regular', 'Playoffs'), used for grouping or filtering games by season type.
- **game_date**: The date on which the game was played, crucial for time-based analysis and filtering in WHERE clauses.
- **game_time**: The time the game started, often used in conjunction with game_date for precise event timing.
- **arena_name**: The name of the arena where the game was held, useful for location-based analysis or reporting.
- **home_team_id**: Identifier for the home team, used in JOINs with team dimension tables to retrieve team details.
- **visitor_team_id**: Identifier for the visiting team, similar use as home_team_id for team-related JOINs.
- **home_score**: The score achieved by the home team, used in SELECT for performance analysis.
- **visitor_score**: The score achieved by the visiting team, also used in SELECT for comparative analysis.
- **game_type**: Specifies the type of game (e.g., 'Regular', 'Playoff'), important for filtering and grouping.
- **playoff_round**: Indicates the playoff round, relevant for playoff-specific queries and analysis.

## Common Query Patterns

- Retrieve all games played in a specific season: `WHERE season_year = '2023'`
- Analyze game outcomes by team: `SELECT home_team_id, visitor_team_id, home_score, visitor_score`
- Filter games by type and date: `WHERE game_type = 'Playoff' AND game_date BETWEEN '2023-04-01' AND '2023-06-01'`
- Compare scores for home and visitor teams: `SELECT game_id, home_score, visitor_score WHERE home_score > visitor_score`

## Join Relationships

- **home_team_id** and **visitor_team_id** are typically joined with a team dimension table to fetch detailed team information.
- **game_id** can be used to join with other fact tables that store detailed player statistics or event logs for the same games.