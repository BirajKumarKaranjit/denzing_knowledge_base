---
name: dwh_d_teams
description: "Use when the query involves retrieving or analyzing team information such as team identity, location, historical data, or current status. This table is essential for understanding team demographics, including their full names, abbreviations, and nicknames, as well as geographical data like city and state. It also provides insights into the team's history with the year founded, and competitive context with conference and division details. The active status column is crucial for filtering current teams from historical data."
tags: [teams, sports, demographics, history, status]
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

- **team_id**: A unique identifier for each team. Typically used in JOIN operations to link with other tables containing team-related data.
- **full_name**: The complete name of the team, such as "Los Angeles Lakers". Useful in SELECT statements for display purposes.
- **abbreviation**: A short form of the team name, like "LAL" for Los Angeles Lakers. Commonly used in reports and dashboards.
- **nickname**: The informal or popular name of the team, such as "Lakers". Often used in SELECT and WHERE clauses for user-friendly outputs.
- **city**: The city where the team is based. Can be used in WHERE clauses to filter teams by location.
- **state**: The state where the team is located. Useful for regional analysis and filtering.
- **year_founded**: The year the team was established. Important for historical analysis and trend reporting.
- **conference**: The conference in which the team competes, such as "Western Conference". Used in GROUP BY or WHERE clauses for competitive analysis.
- **division**: The division within the conference, like "Pacific Division". Helps in detailed competitive breakdowns.
- **active_status**: Indicates whether the team is currently active or not. Critical for filtering out defunct teams in current analyses.

## Common Query Patterns

- Retrieve all active teams in a specific conference: `SELECT * FROM dwh_d_teams WHERE conference = 'Western Conference' AND active_status = 'Active';`
- List all teams founded before a certain year: `SELECT full_name FROM dwh_d_teams WHERE year_founded < '2000';`
- Find teams based in a specific city: `SELECT nickname FROM dwh_d_teams WHERE city = 'Los Angeles';`
- Aggregate teams by division: `SELECT division, COUNT(*) FROM dwh_d_teams GROUP BY division;`

## Join Relationships

- Typically joined with a fact table containing game or player statistics using `team_id`.
- Can be linked to a location dimension table using `city` and `state` for more detailed geographical analysis.
- Often joined with a historical performance table using `team_id` to analyze team performance over time.