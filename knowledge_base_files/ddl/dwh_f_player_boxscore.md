---
name: dwh_f_player_boxscore
description: "Use when the query involves detailed player performance statistics from individual NBA games. This table provides comprehensive box score data, including shooting efficiency, scoring breakdowns, and advanced metrics like usage percentage and player impact estimate (PIE). It's ideal for analyzing player contributions, comparing performances across games, or evaluating specific aspects like shooting accuracy or defensive impact. Commonly used in queries about player stats, game analysis, and team performance evaluations."
tags: [player, boxscore, performance, statistics]
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

- **id**: Unique identifier for each box score entry. Used in SELECT and WHERE clauses.
- **game_id**: Identifier linking to the specific game. Essential for joining with game-related tables.
- **team_id**: Identifier for the team the player belongs to. Useful for team-based queries.
- **player_id**: Unique identifier for the player. Critical for player-specific analysis.
- **position**: Player's position (e.g., Guard, Forward). Helps in role-based performance analysis.
- **comment**: Additional notes or comments about the player's performance. Nullable.
- **jerseynum**: Player's jersey number. Typically used for display purposes.
- **minutes**: Total minutes played by the player. Key metric for evaluating playing time.
- **field_goals_made**: Number of field goals made. Used in calculating shooting efficiency.
- **field_goals_attempted**: Number of field goals attempted. Important for analyzing shot selection.
- **three_pointers_made**: Number of three-point shots made. Critical for evaluating perimeter shooting.
- **three_pointers_attempted**: Number of three-point shots attempted. Used to assess shooting volume.
- **free_throws_made**: Number of free throws made. Important for scoring efficiency analysis.
- **free_throws_attempted**: Number of free throws attempted. Used to evaluate free throw opportunities.
- **rebounds_offensive**: Offensive rebounds secured. Key for assessing second-chance opportunities.
- **rebounds_defensive**: Defensive rebounds secured. Important for evaluating defensive presence.
- **assists**: Number of assists made. Used to measure playmaking ability.
- **steals**: Number of steals. Important for defensive impact analysis.
- **blocks**: Number of blocks. Used to evaluate rim protection.
- **turnovers**: Number of turnovers committed. Critical for assessing ball security.
- **fouls_personal**: Number of personal fouls. Important for understanding foul trouble.
- **points**: Total points scored. Primary metric for scoring analysis.
- **plus_minus_points**: Plus/minus points differential. Used to evaluate overall impact on the game.
- **percentage_field_goals_attempted_2pt**: Percentage of field goals attempted that are 2-pointers. Used for shot distribution analysis.
- **percentage_field_goals_attempted_3pt**: Percentage of field goals attempted that are 3-pointers. Important for perimeter shooting strategy.
- **percentage_points_2pt**: Percentage of total points from 2-point shots. Used for scoring breakdown.
- **percentage_points_midrange_2pt**: Percentage of total points from mid-range 2-point shots. Important for mid-range scoring analysis.
- **percentage_points_3pt**: Percentage of total points from 3-point shots. Critical for assessing three-point contribution.
- **percentage_points_fastbreak**: Percentage of total points from fast breaks. Used to evaluate transition offense.
- **percentage_points_free_throw**: Percentage of total points from free throws. Important for free throw scoring analysis.
- **percentage_points_off_turnovers**: Percentage of total points from turnovers. Used to assess capitalizing on opponent mistakes.
- **percentage_points_paint**: Percentage of total points scored in the paint. Important for interior scoring analysis.
- **percentage_assisted_2pt**: Percentage of 2-point field goals that were assisted. Used to evaluate team play.
- **percentage_unassisted_2pt**: Percentage of 2-point field goals that were unassisted. Important for individual scoring ability.
- **percentage_assisted_3pt**: Percentage of 3-point field goals that were assisted. Used to assess team shooting dynamics.
- **percentage_unassisted_3pt**: Percentage of 3-point field goals that were unassisted. Important for individual shooting prowess.
- **percentage_assisted_fgm**: Percentage of all field goals that were assisted. Key for understanding team play style.
- **percentage_unassisted_fgm**: Percentage of all field goals that were unassisted. Important for individual scoring analysis.
- **free_throw_attempt_rate**: Ratio of free throw attempts to field goal attempts. Used to evaluate aggressiveness in drawing fouls.
- **offensive_rebound_percentage**: Percentage of available offensive rebounds secured. Key for second-chance opportunities.
- **estimated_offensive_rating**: Estimated points produced per 100 possessions. Used for offensive efficiency analysis.
- **offensive_rating**: Actual points produced per 100 possessions. Critical for evaluating offensive performance.
- **estimated_defensive_rating**: Estimated points allowed per 100 possessions. Used for defensive efficiency analysis.
- **defensive_rating**: Actual points allowed per 100 possessions. Important for evaluating defensive performance.
- **assist_percentage**: Percentage of teammate field goals a player assisted while on the floor. Key for playmaking analysis.
- **assist_ratio**: Assists per 100 possessions. Used to evaluate passing efficiency.
- **defensive_rebound_percentage**: Percentage of available defensive rebounds secured. Important for defensive rebounding analysis.
- **rebound_percentage**: Total rebounds secured as a percentage of available rebounds. Key for overall rebounding performance.
- **turnover_ratio**: Turnovers per 100 possessions. Critical for assessing ball control.
- **usage_percentage**: Percentage of team plays used by a player while on the floor. Important for understanding player involvement.
- **estimated_usage_percentage**: Estimated percentage of team plays used by a player. Used for player involvement analysis.
- **pie**: Player Impact Estimate, a measure of a player's overall statistical contribution. Key for evaluating overall impact.
- **technical_foul_count**: Number of technical fouls received. Important for disciplinary analysis.
- **technical_foul_description1**: Description of the first technical foul. Provides context for technical fouls.
- **technical_foul_description2**: Description of the second technical foul. Provides additional context for technical fouls.

## Common Query Patterns

- Retrieve player performance for a specific game: `SELECT * FROM dwh_f_player_boxscore WHERE game_id = '20231025' AND player_id = '12345';`
- Analyze shooting efficiency across games: `SELECT player_id, AVG(field_goals_made/field_goals_attempted) AS fg_percentage FROM dwh_f_player_boxscore GROUP BY player_id;`
- Compare player impact using PIE: `SELECT player_id, AVG(pie) FROM dwh_f_player_boxscore WHERE team_id = '67890' GROUP BY player_id;`
- Evaluate team performance in a specific game: `SELECT SUM(points) FROM dwh_f_player_boxscore WHERE game_id = '20231025' AND team_id = '67890';`

## Join Relationships

- **game_id**: Typically joined with a game table to retrieve game-specific details.
- **team_id**: Joined with a team table to get team information and context.
- **player_id**: Joined with a player table to access player demographics and career stats.
- **id**: Used as a primary key for unique identification within this table, though not typically joined with other tables.