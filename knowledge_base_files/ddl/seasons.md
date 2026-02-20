---
name: seasons
description: "Use when the query involves retrieving information about specific NBA seasons, such as the year a season took place, its start and end dates, playoff start dates, or identifying the champion team and MVP player for that season. This table is essential for historical analysis of NBA seasons, understanding season timelines, and linking seasonal achievements to teams and players. It is particularly useful for queries involving season-specific statistics, awards, and timelines."
tags: [seasons, nba, timeline, champions, mvp]
priority: medium
---

# DDL

```sql
CREATE TABLE seasons (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year     INT NOT NULL UNIQUE,        -- e.g., 2023 for the 2023-24 season
    start_date      DATE NOT NULL,
    end_date        DATE NOT NULL,
    playoff_start   DATE,
    champion_team_id UUID REFERENCES teams(id),
    mvp_player_id   UUID REFERENCES players(id),
    created_at      TIMESTAMP DEFAULT NOW()
);
```

## Column Semantics

- **id**: A unique identifier for each season, used internally for database operations.
- **season_year**: Represents the starting year of the NBA season, such as 2023 for the 2023-24 season. This is a key attribute for filtering and grouping data by season.
- **start_date**: The official start date of the NBA season, typically in October. Useful for timeline analysis and determining season duration.
- **end_date**: The official end date of the NBA season, usually in April before playoffs. Important for calculating season length and scheduling.
- **playoff_start**: The date when the playoffs begin, typically in April. Nullable as it may not be set until the regular season concludes.
- **champion_team_id**: References the team that won the NBA Championship for the season. Links to the `teams` table and is crucial for identifying championship teams.
- **mvp_player_id**: References the player awarded the Most Valuable Player (MVP) for the season. Links to the `players` table, essential for recognizing individual achievements.
- **created_at**: Timestamp of when the record was created, useful for auditing and tracking changes over time.

## Common Query Patterns

- Retrieve all seasons with their start and end dates for a timeline overview.
- Identify the champion team and MVP player for a specific season year.
- Filter seasons to find those where a specific team won the championship.
- Analyze the duration of seasons by comparing start and end dates.

## Join Relationships

- **teams**: Join on `champion_team_id` to get details about the championship-winning team.
- **players**: Join on `mvp_player_id` to get details about the MVP player for the season.
- This table serves as a bridge for connecting seasonal data with team and player achievements.