---
name: games
description: "Use when the query involves retrieving information about specific NBA games, including details such as game dates, types (regular, playoff, preseason), scores, and participating teams. This table is essential for analyzing game outcomes, determining home and away team performances, and understanding game attendance trends. It is also useful for queries involving overtime games, identifying winning teams, and linking games to specific arenas."
tags: [games, nba, scores, teams, attendance]
priority: high
---

# DDL

```sql

CREATE TABLE games (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    game_date       DATE NOT NULL,
    season_year     INT NOT NULL,               -- e.g., 2023 means 2023-24 season
    game_type       VARCHAR(20) NOT NULL,       -- 'regular', 'playoff', 'preseason'
    home_team_id    UUID NOT NULL REFERENCES teams(id),
    away_team_id    UUID NOT NULL REFERENCES teams(id),
    home_score      INT,
    away_score      INT,
    winner_team_id  UUID REFERENCES teams(id),
    overtime        BOOLEAN DEFAULT FALSE,
    attendance      INT,
    arena_id        UUID REFERENCES arenas(id),
    created_at      TIMESTAMP DEFAULT NOW()
);

```

## Column Semantics

- **id**: Unique identifier for each game. Used in SELECT and JOIN operations.
- **game_date**: The date when the game was played. Commonly used in WHERE clauses to filter games by date.
- **season_year**: Indicates the NBA season, e.g., 2023 for the 2023-24 season. Useful for grouping or filtering games by season.
- **game_type**: Specifies if the game is 'regular', 'playoff', or 'preseason'. Important for filtering games based on type.
- **home_team_id**: References the home team. Used in JOINs with the teams table to get team details.
- **away_team_id**: References the away team. Also used in JOINs with the teams table.
- **home_score**: The final score of the home team. Used in SELECT to retrieve game results.
- **away_score**: The final score of the away team. Similar use as home_score.
- **winner_team_id**: References the winning team. Useful for determining game outcomes.
- **overtime**: Indicates if the game went into overtime. Boolean value, often used in WHERE clauses.
- **attendance**: Number of attendees at the game. Can be used for analyzing fan engagement.
- **arena_id**: References the arena where the game was played. Used in JOINs with the arenas table.
- **created_at**: Timestamp of when the record was created. Typically not used in queries.

## Common Query Patterns

- Retrieve all games for a specific season: `WHERE season_year = 2023`
- Find games that went into overtime: `WHERE overtime = TRUE`
- Get the scores and winner for a specific game date: `WHERE game_date = '2023-12-25'`
- Analyze attendance trends by season or game type: `GROUP BY season_year, game_type`

## Join Relationships

- **teams**: Join on `home_team_id` or `away_team_id` to get team names and details.
- **arenas**: Join on `arena_id` to get arena information.
- **teams**: Join on `winner_team_id` to identify the winning team details.