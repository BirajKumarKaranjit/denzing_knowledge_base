---
name: salaries
description: "Use when the query involves player salaries, contract details, or team payrolls for specific NBA seasons. This table is essential for analyzing player compensation, understanding team salary cap situations, and evaluating contract types such as max contracts or rookie deals. It is useful for queries about financial commitments of teams, comparisons of player earnings across seasons, and determining guaranteed versus non-guaranteed contracts. Ideal for financial analysis and roster management discussions."
tags: [salaries, contracts, nba, payroll]
priority: high
---

# DDL

```sql

CREATE TABLE salaries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id       UUID NOT NULL REFERENCES players(id),
    team_id         UUID NOT NULL REFERENCES teams(id),
    season_year     INT NOT NULL,
    salary_usd      BIGINT NOT NULL,            -- Annual salary in USD (not millions)
    contract_type   VARCHAR(50),               -- 'max', 'rookie', 'veteran_min', 'mid_level'
    guaranteed      BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (player_id, season_year)
);

```

## Column Semantics

- **id**: Unique identifier for each salary record. Typically used in SELECT queries for specific record retrieval.
- **player_id**: References the player receiving the salary. Used in JOINs with the players table to get player details.
- **team_id**: References the team paying the salary. Used in JOINs with the teams table to analyze team payrolls.
- **season_year**: Indicates the NBA season (e.g., 2023 for the 2022-2023 season). Commonly used in WHERE clauses to filter by season.
- **salary_usd**: The player's annual salary in USD. Values range from minimum salary levels to multi-million dollar contracts. Used in SELECT and ORDER BY for financial analysis.
- **contract_type**: Describes the type of contract, such as 'max', 'rookie', 'veteran_min', or 'mid_level'. Useful for filtering in WHERE clauses to analyze specific contract types.
- **guaranteed**: Indicates if the salary is guaranteed. Boolean value, often used in WHERE clauses to filter guaranteed versus non-guaranteed contracts.
- **created_at**: Timestamp of when the record was created. Typically used for auditing purposes.

## Common Query Patterns

- Retrieve total payroll for a team in a specific season: `SELECT SUM(salary_usd) FROM salaries WHERE team_id = ? AND season_year = ?;`
- Compare player salaries across different seasons: `SELECT player_id, season_year, salary_usd FROM salaries WHERE player_id = ? ORDER BY season_year;`
- Identify players with max contracts in a given season: `SELECT player_id FROM salaries WHERE contract_type = 'max' AND season_year = ?;`
- Analyze guaranteed salary commitments for a team: `SELECT SUM(salary_usd) FROM salaries WHERE team_id = ? AND guaranteed = TRUE;`

## Join Relationships

- **players**: Join on `player_id` to access player-specific details such as name, position, and statistics.
- **teams**: Join on `team_id` to access team-specific information like team name, location, and conference.
- Typically joined with the `players` table to get player names and with the `teams` table to analyze team payrolls and salary cap situations.