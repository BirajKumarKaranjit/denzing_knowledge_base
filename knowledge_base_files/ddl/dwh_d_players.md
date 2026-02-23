---
name: dwh_d_players
description: "Use when the query involves retrieving detailed information about NBA players, such as their personal details, career history, and draft information. This table is essential for queries that need to filter or group players by team, position, or experience level. It is also useful for analyzing player demographics, such as country of origin or educational background, and for identifying players who are part of the NBA's 75 greatest players list. Ideal for constructing player profiles or conducting historical player comparisons."
tags: [players, demographics, career, draft]
priority: medium
---

# DDL

```sql
CREATE TABLE dwh_d_players (
    player_id text,
    team_id text,
    full_name text,
    player_slug text,
    birthdate date,
    school text,
    country text,
    last_affiliation text,
    player_height numeric,
    player_weight numeric,
    season_experience numeric,
    jersey_number text,
    position text,
    roster_status text,
    from_year text,
    to_year text,
    draft_year text,
    draft_round text,
    draft_number text,
    greatest_75_flag text
);
```

## Column Semantics

- **player_id**: Unique identifier for each player. Used in joins and WHERE clauses.
- **team_id**: Identifier for the team the player is associated with. Useful for joining with team tables.
- **full_name**: The player's full name, e.g., "LeBron James". Used in SELECT and WHERE for display and filtering.
- **player_slug**: A URL-friendly version of the player's name, often used in web applications.
- **birthdate**: The player's date of birth. Useful for calculating age or filtering by age range.
- **school**: The college or high school the player attended. Important for analyzing player development.
- **country**: The player's country of origin. Useful for demographic analysis.
- **last_affiliation**: The last team or league the player was affiliated with before joining the NBA.
- **player_height**: Height in inches. Used in SELECT for player profiles.
- **player_weight**: Weight in pounds. Often used alongside height for physical analysis.
- **season_experience**: Number of seasons the player has played in the NBA. Used in WHERE or SELECT for experience-based queries.
- **jersey_number**: The player's jersey number. Often used in SELECT for display purposes.
- **position**: The player's position, e.g., "Guard", "Forward". Used in WHERE and GROUP BY for role-based analysis.
- **roster_status**: Indicates if the player is active, inactive, or retired. Critical for filtering current players.
- **from_year**: The year the player started their NBA career. Useful for historical analysis.
- **to_year**: The year the player ended their NBA career. Important for career span analysis.
- **draft_year**: The year the player was drafted. Used in historical draft analysis.
- **draft_round**: The round in which the player was drafted. Important for draft strategy analysis.
- **draft_number**: The overall pick number in the draft. Used for evaluating draft success.
- **greatest_75_flag**: Indicates if the player is part of the NBA's 75 greatest players list. Used in SELECT for special recognition.

## Common Query Patterns

- Retrieve all players currently active on a specific team: `WHERE team_id = 'XYZ' AND roster_status = 'active'`
- List players drafted in a specific year and round: `WHERE draft_year = '2020' AND draft_round = '1'`
- Analyze player demographics by country: `SELECT country, COUNT(*) FROM dwh_d_players GROUP BY country`
- Identify players with more than 10 years of experience: `WHERE season_experience > 10`

## Join Relationships

- **team_id**: Typically joined with a team dimension table to get team details.
- **player_id**: Used to join with performance or statistics tables to analyze player performance.
- **draft_year, draft_round, draft_number**: Can be joined with draft tables for detailed draft analysis.