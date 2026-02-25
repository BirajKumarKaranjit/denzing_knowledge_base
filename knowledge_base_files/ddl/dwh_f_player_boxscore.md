---
name: dwh_f_player_boxscore
description: "Use when the query involves detailed player performance metrics in basketball games, such as shooting efficiency, scoring distribution, and advanced analytics like usage percentage and player impact estimate (PIE). This table is essential for analyzing individual player contributions, comparing player performances across games, and understanding the impact of specific actions like assists and turnovers. It is particularly useful for generating player statistics reports, conducting game-by-game performance analysis, and evaluating player efficiency and effectiveness in various game situations."
tags: [basketball, player performance, analytics, boxscore]
priority: high
---

# DDL

```sql
CREATE TABLE dwh_f_player_boxscore (
    id text,
    game_id text,
    team_id text,
    player_id text,
    position text,
    comment text,
    jerseynum text,
    minutes numeric,
    field_goals_made numeric,
    field_goals_attempted numeric,
    three_pointers_made numeric,
    three_pointers_attempted numeric,
    free_throws_made numeric,
    free_throws_attempted numeric,
    rebounds_offensive numeric,
    rebounds_defensive numeric,
    assists numeric,
    steals numeric,
    blocks numeric,
    turnovers numeric,
    fouls_personal numeric,
    points numeric,
    plus_minus_points numeric,
    percentage_field_goals_attempted_2pt numeric,
    percentage_field_goals_attempted_3pt numeric,
    percentage_points_2pt numeric,
    percentage_points_midrange_2pt numeric,
    percentage_points_3pt numeric,
    percentage_points_fastbreak numeric,
    percentage_points_free_throw numeric,
    percentage_points_off_turnovers numeric,
    percentage_points_paint numeric,
    percentage_assisted_2pt numeric,
    percentage_unassisted_2pt numeric,
    percentage_assisted_3pt numeric,
    percentage_unassisted_3pt numeric,
    percentage_assisted_fgm numeric,
    percentage_unassisted_fgm numeric,
    free_throw_attempt_rate numeric,
    offensive_rebound_percentage numeric,
    estimated_offensive_rating numeric,
    offensive_rating numeric,
    estimated_defensive_rating numeric,
    defensive_rating numeric,
    assist_percentage numeric,
    assist_ratio numeric,
    defensive_rebound_percentage numeric,
    rebound_percentage numeric,
    turnover_ratio numeric,
    usage_percentage numeric,
    estimated_usage_percentage numeric,
    pie numeric,
    technical_foul_count numeric,
    technical_foul_description1 text,
    technical_foul_description2 text
);
```

## Column Semantics

- **id**: Unique identifier for each boxscore entry. Typically used in SELECT and WHERE clauses.
- **game_id**: Identifier for the game. Used to group or filter data by specific games.
- **team_id**: Identifier for the team. Useful in JOIN operations with team-related tables.
- **player_id**: Identifier for the player. Essential for filtering data by player.
- **position**: Player's position (e.g., Guard, Forward). Used in SELECT for position-based analysis.
- **comment**: Additional notes or comments about the player's performance. Nullable.
- **jerseynum**: Player's jersey number. Used for display purposes.
- **minutes**: Total minutes played by the player. A key metric in performance analysis.
- **field_goals_made**: Number of field goals made. Used in calculating shooting efficiency.
- **field_goals_attempted**: Number of field goals attempted. Important for efficiency metrics.
- **three_pointers_made**: Number of three-point shots made. Used in three-point shooting analysis.
- **three_pointers_attempted**: Number of three-point shots attempted. Important for efficiency metrics.
- **free_throws_made**: Number of free throws made. Used in free throw efficiency analysis.
- **free_throws_attempted**: Number of free throws attempted. Important for efficiency metrics.
- **rebounds_offensive**: Number of offensive rebounds. Used in evaluating rebounding performance.
- **rebounds_defensive**: Number of defensive rebounds. Important for defensive performance analysis.
- **assists**: Number of assists. Key metric for playmaking analysis.
- **steals**: Number of steals. Used in defensive performance evaluation.
- **blocks**: Number of blocks. Important for defensive impact analysis.
- **turnovers**: Number of turnovers. Used in assessing ball-handling efficiency.
- **fouls_personal**: Number of personal fouls. Important for foul management analysis.
- **points**: Total points scored by the player. A primary performance metric.
- **plus_minus_points**: Plus/minus points differential. Used in evaluating overall impact.
- **percentage_field_goals_attempted_2pt**: Percentage of field goals attempted that are 2-point shots.
- **percentage_field_goals_attempted_3pt**: Percentage of field goals attempted that are 3-point shots.
- **percentage_points_2pt**: Percentage of total points from 2-point shots.
- **percentage_points_midrange_2pt**: Percentage of points from midrange 2-point shots.
- **percentage_points_3pt**: Percentage of total points from 3-point shots.
- **percentage_points_fastbreak**: Percentage of points from fast breaks.
- **percentage_points_free_throw**: Percentage of total points from free throws.
- **percentage_points_off_turnovers**: Percentage of points scored off turnovers.
- **percentage_points_paint**: Percentage of points scored in the paint.
- **percentage_assisted_2pt**: Percentage of 2-point field goals that were assisted.
- **percentage_unassisted_2pt**: Percentage of 2-point field goals that were unassisted.
- **percentage_assisted_3pt**: Percentage of 3-point field goals that were assisted.
- **percentage_unassisted_3pt**: Percentage of 3-point field goals that were unassisted.
- **percentage_assisted_fgm**: Percentage of all field goals made that were assisted.
- **percentage_unassisted_fgm**: Percentage of all field goals made that were unassisted.
- **free_throw_attempt_rate**: Rate of free throw attempts relative to field goal attempts.
- **offensive_rebound_percentage**: Percentage of available offensive rebounds grabbed.
- **estimated_offensive_rating**: Estimated points produced per 100 possessions.
- **offensive_rating**: Actual points produced per 100 possessions.
- **estimated_defensive_rating**: Estimated points allowed per 100 possessions.
- **defensive_rating**: Actual points allowed per 100 possessions.
- **assist_percentage**: Percentage of teammate field goals a player assisted while on the floor.
- **assist_ratio**: Assists per 100 possessions.
- **defensive_rebound_percentage**: Percentage of available defensive rebounds grabbed.
- **rebound_percentage**: Overall percentage of available rebounds grabbed.
- **turnover_ratio**: Turnovers per 100 possessions.
- **usage_percentage**: Percentage of team plays used by a player while on the floor.
- **estimated_usage_percentage**: Estimated percentage of team plays used by a player.
- **pie**: Player Impact Estimate, a measure of a player's overall statistical contribution.
- **technical_foul_count**: Number of technical fouls received. Important for disciplinary analysis.
- **technical_foul_description1**: Description of the first technical foul. Nullable.
- **technical_foul_description2**: Description of the second technical foul. Nullable.

## Common Query Patterns

- Retrieve player performance metrics for a specific game: `SELECT * FROM dwh_f_player_boxscore WHERE game_id = 'game123';`
- Analyze shooting efficiency for a player across multiple games: `SELECT player_id, AVG(field_goals_made/field_goals_attempted) AS shooting_percentage FROM dwh_f_player_boxscore WHERE player_id = 'player456' GROUP BY player_id;`
- Compare team performance in terms of rebounds and assists: `SELECT team_id, SUM(rebounds_offensive + rebounds_defensive) AS total_rebounds, SUM(assists) AS total_assists FROM dwh_f_player_boxscore GROUP BY team_id;`
- Evaluate player impact using plus/minus points: `SELECT player_id, SUM(plus_minus_points) AS total_plus_minus FROM dwh_f_player_boxscore GROUP BY player_id;`

## Join Relationships

- **game_id**: Typically joined with a games table to retrieve game-specific details.
- **team_id**: Joined with a teams table to get team information and statistics.
- **player_id**: Joined with a players table to access player profiles and historical data.