---
name: dwh_d_games
description: "Use when the query involves retrieving detailed information about NBA games, such as game schedules, scores, and team matchups. This table is essential for analyzing game outcomes, comparing team performances, and understanding game contexts like season type and playoff rounds. It is particularly useful for queries that need to distinguish between regular season and playoff games, or when examining game-specific details like the arena or game timing."
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
- **gamecode**: A code representing the game, often used for internal tracking or referencing in other systems.
- **season_year**: The year in which the season starts, e.g., '2023'. Useful for filtering games by season.
- **season**: Describes the part of the NBA season, such as 'Regular Season' or 'Playoffs'. Important for distinguishing game contexts.
- **game_date**: The date the game was played. Commonly used in WHERE clauses to filter games by date range.
- **game_time**: The time the game started, local to the arena's time zone. Used less frequently but can be important for time-specific analyses.
- **arena_name**: The name of the arena where the game was played. Useful for location-based queries or analyses.
- **home_team_id**: The identifier for the home team. Essential for JOINs with team tables to get team details.
- **visitor_team_id**: The identifier for the visiting team. Also used in JOINs with team tables.
- **home_score**: The final score of the home team. Used in SELECT statements to analyze game outcomes.
- **visitor_score**: The final score of the visiting team. Similarly used to determine game results.
- **game_type**: Indicates the type of game, such as 'Regular' or 'Playoff'. Important for filtering and analysis.
- **playoff_round**: Specifies the playoff round if applicable, e.g., 'First Round', 'Finals'. Critical for playoff-specific analyses.

## Common Query Patterns

- Retrieve all games for a specific season and team: `WHERE season_year = '2023' AND (home_team_id = 'XYZ' OR visitor_team_id = 'XYZ')`
- Analyze game outcomes by comparing home and visitor scores: `SELECT game_id, home_score, visitor_score WHERE home_score > visitor_score`
- Filter games by type and playoff round: `WHERE game_type = 'Playoff' AND playoff_round = 'Finals'`
- List games played at a specific arena: `WHERE arena_name = 'Staples Center'`

## Join Relationships

- Typically joined with a team dimension table using `home_team_id` and `visitor_team_id` to get detailed team information.
- Can be joined with a player statistics table using `game_id` to analyze player performances in specific games.
- Often used in conjunction with a calendar table to enrich date-based analyses.