---
name: dwh_d_players
description: "Use when the query involves player demographics, career history, or team affiliations in an analytics context. This table provides detailed information about players, including their physical attributes, career timeline, and draft details, which are essential for performance analysis and historical comparisons. It is particularly useful for queries that require filtering by player attributes such as height, weight, or position, and for aggregating statistics based on player experience or draft information. Ideal for generating reports on player career progression, team compositions, or historical player data."
tags: [players, demographics, career, analytics]
priority: medium
fk_to:
  - column: team_id
    ref_table: dwh_d_teams
    ref_column: team_id
related_tables: [dwh_d_teams, dwh_d_player_nicknames]
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
- **team_id**: Identifier for the team the player is associated with. Commonly used in joins with team tables.
- **full_name**: The player's full name. Useful for display purposes and SELECT statements.
- **player_slug**: A URL-friendly version of the player's name. Used in web applications and SELECT statements.
- **birthdate**: The player's date of birth. Can be used to calculate age or filter players by age range.
- **school**: The educational institution the player attended. Useful for demographic analysis.
- **country**: The player's country of origin. Important for international player analysis.
- **last_affiliation**: The last team or organization the player was affiliated with before joining the current team. Used in historical analysis.
- **player_height**: The player's height, typically in inches or centimeters. Used in SELECT and WHERE clauses for physical attribute analysis.
- **player_weight**: The player's weight, typically in pounds or kilograms. Used similarly to player_height.
- **season_experience**: Number of seasons the player has participated in. Useful for experience-based analysis.
- **jersey_number**: The player's jersey number. Often used in SELECT statements for display.
- **position**: The player's position on the team (e.g., guard, forward). Critical for role-based analysis.
- **roster_status**: Indicates if the player is active, inactive, or on a different status. Used in WHERE clauses.
- **from_year**: The year the player started their professional career. Useful for career duration analysis.
- **to_year**: The year the player ended their professional career. Used similarly to from_year.
- **draft_year**: The year the player was drafted. Important for draft analysis.
- **draft_round**: The round in which the player was drafted. Used in draft-related queries.
- **draft_number**: The overall pick number of the player in the draft. Used in SELECT and WHERE clauses.
- **greatest_75_flag**: Indicates if the player is part of the league's greatest 75 players. Used for historical significance analysis.

## Common Query Patterns

- Retrieve all players from a specific team: `SELECT * FROM dwh_d_players WHERE team_id = 'XYZ';`
- Find players drafted in a specific year and round: `SELECT full_name FROM dwh_d_players WHERE draft_year = '2020' AND draft_round = '1';`
- List players with more than 10 years of experience: `SELECT full_name FROM dwh_d_players WHERE season_experience > 10;`
- Filter players by position and roster status: `SELECT full_name FROM dwh_d_players WHERE position = 'Guard' AND roster_status = 'Active';`

## Join Relationships

- **team_id**: Typically joined with a team dimension table to get detailed team information.
- **player_id**: Used to join with other player-related tables, such as performance statistics or contract details.
- **country**: Can be joined with a country reference table for additional geographic data.

## Column Notes

- team_id: represents the player's CURRENT team only (point-in-time snapshot).
  Do NOT use this to determine which team a player was on in a past season.
  For historical team membership use dwh_f_player_team_seasons.team_id
  (one row per player per season per team).
  For game-level team use dwh_f_player_boxscore.team_id.