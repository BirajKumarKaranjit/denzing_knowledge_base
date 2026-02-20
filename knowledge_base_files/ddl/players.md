---
name: players
description: "Use this when the user asks about player profiles, player names,
              positions, teams, physical attributes, draft information."
tags: ["players", "roster", "nba"]
priority: high
---
# DDL

```sql
CREATE TABLE players (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name       VARCHAR(200) NOT NULL,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    date_of_birth   DATE,
    nationality     VARCHAR(100),
    position        VARCHAR(10),        -- PG, SG, SF, PF, C, G, F, G-F, F-C
    height_cm       DECIMAL(5,2),
    weight_kg       DECIMAL(5,2),
    jersey_number   INT,
    is_active       BOOLEAN DEFAULT TRUE,
    draft_year      INT,
    draft_round     INT,
    draft_pick      INT,
    team_id         UUID REFERENCES teams(id),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);
```

## Column Semantics

- **id**: Unique identifier for each player, used as a primary key.
- **full_name**: The complete name of the player, used for display and search purposes.
- **first_name** / **last_name**: Components of the player's name, useful for sorting and filtering.
- **date_of_birth**: Player's birthdate, used to calculate age and eligibility.
- **nationality**: Country of origin, often used in demographic analyses.
- **position**: Player's role on the court, such as PG (Point Guard) or C (Center). Key for lineup and strategy queries.
- **height_cm** / **weight_kg**: Physical attributes, important for scouting and performance analysis.
- **jersey_number**: The number worn by the player, often used in fan queries and merchandise.
- **is_active**: Indicates if the player is currently active in the league, crucial for current roster queries.
- **draft_year** / **draft_round** / **draft_pick**: Details of the player's entry into the NBA, used in historical and performance analyses.
- **team_id**: Foreign key linking to the teams table, essential for joining player data with team information.
- **created_at** / **updated_at**: Timestamps for record management, not typically used in analytical queries.

## Common Query Patterns

- Retrieve active players for a specific team: `SELECT * FROM players WHERE team_id = ? AND is_active = TRUE;`
- List players by position and height: `SELECT full_name, height_cm FROM players WHERE position = 'C' ORDER BY height_cm DESC;`
- Analyze draft history: `SELECT draft_year, COUNT(*) FROM players WHERE draft_round = 1 GROUP BY draft_year;`
- Find players by nationality: `SELECT full_name FROM players WHERE nationality = 'USA';`

## Join Relationships

- **teams**: Join on `team_id` to get team details for each player, such as `SELECT p.full_name, t.team_name FROM players p JOIN teams t ON p.team_id = t.id;`
- This table can be joined with game statistics tables using the `id` to analyze player performance metrics.