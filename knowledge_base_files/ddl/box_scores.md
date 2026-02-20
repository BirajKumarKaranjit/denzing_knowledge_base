---
name: box_scores
description: "Use when the query involves detailed player performance statistics for specific NBA games. This table captures individual player contributions such as points, rebounds, assists, and other key metrics during a game. It is essential for analyzing player efficiency, comparing performances across games, and understanding team dynamics. Commonly used in queries involving player stats, game summaries, and team performance analysis."
tags: [player stats, game performance, NBA analytics]
priority: high
---

# DDL

```sql
CREATE TABLE box_scores (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    game_id                 UUID NOT NULL REFERENCES games(id),
    player_id               UUID NOT NULL REFERENCES players(id),
    team_id                 UUID NOT NULL REFERENCES teams(id),
    minutes_played          DECIMAL(5,2),
    points                  INT DEFAULT 0,
    field_goals_made        INT DEFAULT 0,
    field_goal_attempts     INT DEFAULT 0,
    three_pointers_made     INT DEFAULT 0,
    three_point_attempts    INT DEFAULT 0,
    free_throws_made        INT DEFAULT 0,
    free_throw_attempts     INT DEFAULT 0,
    offensive_rebounds      INT DEFAULT 0,
    defensive_rebounds      INT DEFAULT 0,
    total_rebounds          INT DEFAULT 0,
    assists                 INT DEFAULT 0,
    steals                  INT DEFAULT 0,
    blocks                  INT DEFAULT 0,
    turnovers               INT DEFAULT 0,
    personal_fouls          INT DEFAULT 0,
    plus_minus              INT,
    starter                 BOOLEAN DEFAULT FALSE,
    created_at              TIMESTAMP DEFAULT NOW(),
    UNIQUE (game_id, player_id)
);
```

## Column Semantics

- **id**: Unique identifier for each box score entry. Not typically used in queries.
- **game_id**: References the specific game. Used in JOINs with the `games` table to filter or aggregate data by game.
- **player_id**: References the player. Essential for filtering stats by player and joining with the `players` table.
- **team_id**: References the team. Useful for team-based queries and joining with the `teams` table.
- **minutes_played**: Total minutes a player was on the court, typically ranging from 0 to 48 in regulation games.
- **points**: Total points scored by the player. Key metric for evaluating scoring performance.
- **field_goals_made**: Number of successful field goals. Used to calculate shooting efficiency.
- **field_goal_attempts**: Total field goal attempts. Combined with field_goals_made to determine shooting percentage.
- **three_pointers_made**: Successful three-point shots. Important for assessing long-range shooting ability.
- **three_point_attempts**: Total three-point shot attempts. Used to calculate three-point shooting percentage.
- **free_throws_made**: Successful free throws. Part of overall scoring efficiency.
- **free_throw_attempts**: Total free throw attempts. Used to calculate free throw percentage.
- **offensive_rebounds**: Rebounds collected on the offensive end. Indicates second-chance opportunities.
- **defensive_rebounds**: Rebounds collected on the defensive end. Reflects defensive effectiveness.
- **total_rebounds**: Sum of offensive and defensive rebounds. Key for evaluating a player's rebounding ability.
- **assists**: Passes leading directly to a score. Indicates playmaking ability.
- **steals**: Number of times a player takes the ball from an opponent. Reflects defensive prowess.
- **blocks**: Number of shots blocked. Another indicator of defensive ability.
- **turnovers**: Number of times a player loses possession. Used to assess ball-handling and decision-making.
- **personal_fouls**: Fouls committed by the player. Important for understanding playing time and defensive aggression.
- **plus_minus**: Net point differential when the player is on the court. Used to gauge overall impact.
- **starter**: Indicates if the player was in the starting lineup. Useful for lineup analysis.
- **created_at**: Timestamp of record creation. Typically used for auditing or tracking data changes.

## Common Query Patterns

- Retrieve all player stats for a specific game: `SELECT * FROM box_scores WHERE game_id = 'some-game-id';`
- Calculate average points per game for a player: `SELECT AVG(points) FROM box_scores WHERE player_id = 'some-player-id';`
- Compare team performance by aggregating player stats: `SELECT team_id, SUM(points) FROM box_scores WHERE game_id = 'some-game-id' GROUP BY team_id;`
- Identify top performers by filtering on points or other metrics: `SELECT player_id FROM box_scores WHERE points > 20;`

## Join Relationships

- **games**: Join on `game_id` to associate player stats with specific games.
- **players**: Join on `player_id` to get player details and demographics.
- **teams**: Join on `team_id` to analyze team-level performance and rosters.