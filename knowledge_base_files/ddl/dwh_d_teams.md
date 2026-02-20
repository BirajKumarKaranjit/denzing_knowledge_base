---
name: dwh_d_teams
description: "Use when the query involves retrieving information about NBA teams, such as their full names, abbreviations, nicknames, and locations. This table is essential for understanding team-specific data, including historical context like the year a team was founded and its current status in the league. It is particularly useful for queries involving team affiliations, conference and division alignments, and active status checks. Ideal for analyses that require filtering or grouping by team characteristics or for joining with player statistics and game results."
tags: [teams, nba, conference, division, active_status]
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

- **team_id**: A unique identifier for each NBA team. Typically used in JOIN operations with other tables like player statistics or game results.
- **full_name**: The official full name of the team, e.g., "Los Angeles Lakers". Useful in SELECT statements for display purposes.
- **abbreviation**: A short form of the team name, such as "LAL" for Los Angeles Lakers. Commonly used in WHERE clauses for filtering or in SELECT for concise output.
- **nickname**: The team's commonly used nickname, e.g., "Lakers". Often used interchangeably with the full name in casual contexts.
- **city**: The city where the team is based, e.g., "Los Angeles". Useful for geographical analyses or filtering.
- **state**: The state where the team is located, e.g., "California". Can be used for regional analyses.
- **year_founded**: The year the team was established. Useful for historical analyses or filtering teams by age.
- **conference**: The conference to which the team belongs, either "Eastern" or "Western". Essential for grouping or filtering teams by conference.
- **division**: The division within the conference, such as "Pacific". Important for more granular grouping or filtering.
- **active_status**: Indicates whether the team is currently active in the NBA. Typically "active" or "inactive", used in WHERE clauses to filter current teams.

## Common Query Patterns

- Retrieve all active teams in the Western Conference: `SELECT * FROM dwh_d_teams WHERE conference = 'Western' AND active_status = 'active';`
- List all teams founded before 1980: `SELECT full_name FROM dwh_d_teams WHERE year_founded < '1980';`
- Find teams by city and state: `SELECT full_name FROM dwh_d_teams WHERE city = 'Los Angeles' AND state = 'California';`
- Group teams by division and count: `SELECT division, COUNT(*) FROM dwh_d_teams GROUP BY division;`

## Join Relationships

- **Player Statistics**: Typically joined on `team_id` to associate players with their respective teams.
- **Game Results**: Joined on `team_id` to link teams with game outcomes.
- **Conference and Division Tables**: May join on `conference` and `division` for detailed league structure analyses.