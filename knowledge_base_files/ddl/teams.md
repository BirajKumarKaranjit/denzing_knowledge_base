---
name: teams
description: "Use when the query involves retrieving or analyzing information about NBA teams, such as team names, abbreviations, cities, conferences, divisions, or head coaches. This table is essential for understanding team-specific data, including historical and current team status, and is often used in conjunction with player statistics or game results. It is particularly useful for queries that need to filter or group data by team attributes like conference or division, or when joining with arenas to get location details."
tags: [teams, nba, conference, division, head_coach]
priority: medium
---

# DDL

```sql
CREATE TABLE teams (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(200) NOT NULL,      -- e.g., 'Los Angeles Lakers'
    abbreviation    VARCHAR(5) NOT NULL,         -- e.g., 'LAL'
    city            VARCHAR(100),
    conference      VARCHAR(10),                 -- 'East' or 'West'
    division        VARCHAR(50),                 -- e.g., 'Pacific', 'Atlantic'
    arena_id        UUID REFERENCES arenas(id),
    head_coach      VARCHAR(200),
    founded_year    INT,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);
```

## Column Semantics

- **id**: Unique identifier for each team. Used in joins and primary key constraints.
- **name**: Full name of the team, such as 'Los Angeles Lakers'. Essential for SELECT queries displaying team names.
- **abbreviation**: Shortened version of the team name, like 'LAL'. Commonly used in reports and visualizations.
- **city**: The city where the team is based. Useful for geographic analysis or filtering.
- **conference**: Indicates whether the team is in the 'East' or 'West' conference. Important for grouping and filtering teams by conference.
- **division**: Specifies the division within the conference, such as 'Pacific' or 'Atlantic'. Used for more granular grouping.
- **arena_id**: Foreign key linking to the arenas table, providing details about the team's home arena. Important for location-based queries.
- **head_coach**: Name of the team's head coach. Useful for historical analysis or current team leadership queries.
- **founded_year**: The year the team was established. Useful for historical context or age-based analysis.
- **is_active**: Indicates if the team is currently active in the league. Used in filtering current vs. historical teams.
- **created_at**: Timestamp of when the record was created. Typically used for auditing or tracking changes over time.

## Common Query Patterns

- Retrieve all active teams in the Western Conference: `SELECT * FROM teams WHERE conference = 'West' AND is_active = TRUE;`
- List teams along with their head coaches: `SELECT name, head_coach FROM teams;`
- Find teams founded before a certain year: `SELECT name FROM teams WHERE founded_year < 2000;`
- Join with arenas to get team and arena details: `SELECT t.name, a.name FROM teams t JOIN arenas a ON t.arena_id = a.id;`

## Join Relationships

- **arenas**: The `arena_id` column is a foreign key referencing the `id` column in the arenas table, allowing you to join teams with their respective arenas to get location and capacity details.