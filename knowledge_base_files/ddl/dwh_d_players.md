---
name: dwh_d_players
description: "Use when the query involves retrieving detailed information about NBA players, including their personal details, career history, and draft information. This table is essential for analyzing player demographics, career progression, and team affiliations. It is useful for queries about player height, weight, position, and experience, as well as for identifying players who are part of the NBA's greatest 75 players list. Ideal for scouting reports, player comparisons, and historical player data analysis."
tags: [players, demographics, career, draft]
priority: high
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

- **player_id**: Unique identifier for each player. Used in JOINs with other tables like game statistics or team rosters.
- **team_id**: Identifier for the team the player is currently or was last associated with. Useful in JOINs with team tables.
- **full_name**: The player's full name, e.g., "LeBron James". Often used in SELECT statements for display purposes.
- **player_slug**: A URL-friendly version of the player's name, typically used in web applications.
- **birthdate**: The player's date of birth. Useful for calculating age or age-related statistics.
- **school**: The college or high school the player attended. Important for scouting and historical analysis.
- **country**: The player's country of origin. Useful for demographic analysis.
- **last_affiliation**: Last team or league affiliation before joining the NBA. Important for understanding player background.
- **player_height**: Height of the player in inches or centimeters. Used in player comparisons and scouting reports.
- **player_weight**: Weight of the player in pounds or kilograms. Relevant for physical assessments.
- **season_experience**: Number of seasons the player has played in the NBA. Important for career analysis.
- **jersey_number**: The player's jersey number, which can change over time. Used in SELECT for display.
- **position**: The player's position on the court, e.g., "Guard", "Forward". Critical for lineup and strategy analysis.
- **roster_status**: Indicates if the player is active, inactive, or retired. Used in WHERE clauses to filter current players.
- **from_year**: The first year the player played in the NBA. Useful for historical queries.
- **to_year**: The last year the player played in the NBA. Important for career span analysis.
- **draft_year**: The year the player was drafted. Used in draft-related queries.
- **draft_round**: The round in which the player was drafted. Important for evaluating draft success.
- **draft_number**: The overall pick number in the draft. Useful for historical draft analysis.
- **greatest_75_flag**: Indicates if the player is part of the NBA's 75th Anniversary Team. Used in WHERE clauses for special lists.

## Common Query Patterns

- Retrieve all players currently active in the NBA: `SELECT * FROM dwh_d_players WHERE roster_status = 'active';`
- Find players drafted in a specific year and round: `SELECT full_name FROM dwh_d_players WHERE draft_year = '2020' AND draft_round = '1';`
- Analyze player demographics by country: `SELECT country, COUNT(*) FROM dwh_d_players GROUP BY country;`
- List players who are part of the NBA's greatest 75 players: `SELECT full_name FROM dwh_d_players WHERE greatest_75_flag = 'Y';`

## Join Relationships

- **team_id**: Typically joined with a team dimension table to get team details.
- **player_id**: Used to join with game statistics tables to analyze player performance.
- **draft_year, draft_round, draft_number**: Can be joined with draft tables for detailed draft analysis.