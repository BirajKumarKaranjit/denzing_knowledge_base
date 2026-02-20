---
name: player_awards
description: "Use when the query involves retrieving or analyzing NBA player awards, such as MVP, Defensive Player of the Year (DPOY), Rookie of the Year (ROY), Sixth Man of the Year (SMOY), All-Star selections, or All-NBA Team honors. This table is essential for understanding a player's accolades over different seasons and can be used to compare award trends across players and teams. It is particularly useful for queries that involve historical performance, player recognition, and team contributions during specific seasons."
tags: [awards, players, NBA, accolades]
priority: high
---

# DDL

```sql

CREATE TABLE player_awards (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id       UUID NOT NULL REFERENCES players(id),
    season_year     INT NOT NULL,
    award_type      VARCHAR(100) NOT NULL,  -- 'MVP', 'DPOY', 'ROY', 'SMOY', 'All-Star', 'All-NBA First Team', etc.
    team_at_time    UUID REFERENCES teams(id),
    created_at      TIMESTAMP DEFAULT NOW()
);

```

## Column Semantics

- **id**: A unique identifier for each award entry. Used primarily for internal tracking and joins.
- **player_id**: References the player who received the award. This is crucial for joining with the `players` table to get player details such as name, position, and career statistics.
- **season_year**: Indicates the NBA season in which the award was received, formatted as a four-digit year (e.g., 2023 for the 2022-2023 season). Commonly used in WHERE clauses to filter awards by season.
- **award_type**: Describes the type of award received. Typical values include 'MVP', 'DPOY', 'ROY', 'SMOY', 'All-Star', and 'All-NBA First Team'. This column is often used in SELECT and WHERE clauses to filter or group by specific awards.
- **team_at_time**: References the team the player was part of when receiving the award. Useful for joining with the `teams` table to analyze team contributions and contexts.
- **created_at**: Timestamp of when the record was created in the database. Generally used for auditing purposes and not typically included in analytical queries.

## Common Query Patterns

- Retrieve all awards for a specific player: `SELECT * FROM player_awards WHERE player_id = ?`
- List all MVPs for a given season: `SELECT player_id FROM player_awards WHERE season_year = ? AND award_type = 'MVP'`
- Count the number of All-Star selections per player: `SELECT player_id, COUNT(*) FROM player_awards WHERE award_type = 'All-Star' GROUP BY player_id`
- Find all awards won by players from a specific team: `SELECT * FROM player_awards WHERE team_at_time = ?`

## Join Relationships

- **players**: Join on `player_awards.player_id = players.id` to get player details.
- **teams**: Join on `player_awards.team_at_time = teams.id` to get team information.
- This table is often joined with `season_stats` to correlate awards with player performance metrics.