---
name: dwh_d_teams
description: "Use when the query involves retrieving information about NBA teams, such as their full names, abbreviations, nicknames, and geographical details like city and state. This table is essential for understanding team-specific data, including historical context with the year founded, and organizational structure with conference and division affiliations. It is also useful for filtering teams based on their active status in the league."
tags: [teams, NBA, conference, division, active status]
priority: medium
---

# DDL

```sql
CREATE TABLE dwh_d_teams (
    team_id text,
    full_name text,
    abbreviation text,
    nickname text,
    city text,
    state text,
    year_founded text,
    conference text,
    division text,
    active_status text
);
```

## Column Semantics

- **team_id**: A unique identifier for each team. Typically used in JOIN operations with other tables to link team-specific data.
- **full_name**: The complete name of the team, such as "Los Angeles Lakers". Used in SELECT statements for display purposes.
- **abbreviation**: A short form of the team name, like "LAL" for Los Angeles Lakers. Commonly used in reports and visualizations.
- **nickname**: The informal name of the team, such as "Lakers". Useful in SELECT and WHERE clauses for user-friendly outputs.
- **city**: The city where the team is based, e.g., "Los Angeles". Often used in geographical analyses or fan base studies.
- **state**: The state where the team is located, such as "California". Useful for regional analyses.
- **year_founded**: The year the team was established, providing historical context. Can be used in historical trend analyses.
- **conference**: The conference to which the team belongs, either "Eastern" or "Western". Important for filtering and grouping teams.
- **division**: The division within the conference, such as "Pacific". Used in grouping and filtering operations.
- **active_status**: Indicates if the team is currently active in the NBA, typically "active" or "inactive". Essential for filtering current teams.

## Common Query Patterns

- Retrieve all active teams in the Western Conference: `SELECT * FROM dwh_d_teams WHERE conference = 'Western' AND active_status = 'active';`
- List all teams founded before 1970: `SELECT full_name FROM dwh_d_teams WHERE year_founded < '1970';`
- Find teams by city: `SELECT full_name FROM dwh_d_teams WHERE city = 'Los Angeles';`
- Get team abbreviations for a specific division: `SELECT abbreviation FROM dwh_d_teams WHERE division = 'Atlantic';`

## Join Relationships

- Typically joined with player tables using `team_id` to associate players with their respective teams.
- Can be joined with game or match tables to filter or group results by team attributes like conference or division.
- Useful in conjunction with historical performance tables to analyze team success over time.