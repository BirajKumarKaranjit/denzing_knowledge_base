---
name: dwh_d_games
description: "Use when the query involves retrieving detailed information about NBA games, including scores, teams, and game logistics. This table is essential for analyzing game outcomes, comparing team performances, and understanding game schedules. It includes data on regular season and playoff games, making it crucial for historical analysis and trend identification across different NBA seasons. Ideal for queries focusing on game results, team matchups, and venue details."
tags: [games, scores, teams, schedule]
priority: high
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

- **game_id**: A unique identifier for each game, typically used in JOINs and WHERE clauses to filter specific games.
- **gamecode**: A code representing the game, often used interchangeably with game_id for identification.
- **season_year**: The year in which the NBA season started, e.g., '2022' for the 2022-2023 season. Useful for filtering data by season.
- **season**: Describes the part of the season, such as 'Regular' or 'Playoffs'. Important for distinguishing between regular season and playoff games.
- **game_date**: The date on which the game was played. Commonly used in WHERE clauses for date filtering.
- **game_time**: The time the game started, typically in local time. Useful for scheduling and time-based analysis.
- **arena_name**: The name of the arena where the game took place. Can be used to analyze home court advantages or venue-specific statistics.
- **home_team_id**: Identifier for the home team, used in JOINs with team tables to get more team details.
- **visitor_team_id**: Identifier for the visiting team, similar usage as home_team_id.
- **home_score**: The final score of the home team. Used in SELECT statements to analyze game outcomes.
- **visitor_score**: The final score of the visiting team. Also used in SELECT statements for outcome analysis.
- **game_type**: Indicates the type of game, such as 'Regular' or 'Playoff'. Important for filtering and analysis based on game significance.
- **playoff_round**: Specifies the playoff round, e.g., 'First Round', 'Finals'. Relevant only for playoff games, used in filtering and analysis of playoff progression.

## Common Query Patterns

- Retrieve all games for a specific season: `WHERE season_year = '2022'`
- Compare scores between home and visitor teams: `SELECT home_score, visitor_score WHERE game_id = '12345'`
- Filter games by type and round: `WHERE game_type = 'Playoff' AND playoff_round = 'Finals'`
- Join with team details: `JOIN team_table ON dwh_d_games.home_team_id = team_table.team_id`

## Join Relationships

- **home_team_id** and **visitor_team_id** typically join with a team dimension table to fetch team names and other details.
- **game_id** can be used to join with player performance tables to analyze individual performances in specific games.
- **arena_name** might join with an arena dimension table for additional venue information.