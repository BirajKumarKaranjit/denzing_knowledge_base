---
name: dwh_f_player_boxscore
description: "Use when the query involves detailed player performance statistics for individual NBA games. This table provides comprehensive box score data, including shooting accuracy, scoring breakdowns, and advanced metrics like usage percentage and player impact estimate (PIE). Ideal for analyzing player contributions in specific games, comparing performance across games, or evaluating player efficiency and effectiveness. It includes both traditional stats like points and rebounds, and advanced analytics such as offensive and defensive ratings."
tags: [player, boxscore, performance, analytics]
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

- **id**: Unique identifier for each box score entry. Used in SELECT and JOIN operations.
- **game_id**: Identifier for the game. Essential for filtering data for specific games.
- **team_id**: Identifier for the team. Useful for team-based analysis and joins with team tables.
- **player_id**: Identifier for the player. Critical for player-specific queries and joins with player tables.
- **position**: Player's position (e.g., Guard, Forward). Useful for filtering and grouping.
- **comment**: Additional notes about the player's performance. Typically used in SELECT for detailed reports.
- **jerseynum**: Player's jersey number. Often used in SELECT for display purposes.
- **minutes**: Total minutes played. Key metric for evaluating player involvement.
- **field_goals_made/attempted**: Number of field goals made/attempted. Used in calculating shooting efficiency.
- **three_pointers_made/attempted**: Number of three-pointers made/attempted. Important for analyzing shooting range.
- **free_throws_made/attempted**: Number of free throws made/attempted. Used in efficiency and scoring analysis.
- **rebounds_offensive/defensive**: Number of offensive/defensive rebounds. Critical for evaluating rebounding performance.
- **assists**: Number of assists. Key for analyzing playmaking ability.
- **steals**: Number of steals. Important for defensive performance analysis.
- **blocks**: Number of blocks. Used in defensive impact evaluation.
- **turnovers**: Number of turnovers. Important for assessing ball control.
- **fouls_personal**: Number of personal fouls. Used in evaluating discipline and defensive aggression.
- **points**: Total points scored. Central metric for scoring performance.
- **plus_minus_points**: Plus/minus score. Indicates team performance while the player is on the court.
- **percentage_field_goals_attempted_2pt/3pt**: Percentage of field goals attempted as 2-pointers/3-pointers. Used in shot selection analysis.
- **percentage_points_2pt/midrange_2pt/3pt/fastbreak/free_throw/off_turnovers/paint**: Breakdown of scoring by type. Useful for detailed scoring analysis.
- **percentage_assisted/unassisted_2pt/3pt/fgm**: Percentage of field goals that were assisted/unassisted. Important for understanding play style.
- **free_throw_attempt_rate**: Ratio of free throw attempts to field goal attempts. Indicates aggressiveness in drawing fouls.
- **offensive/defensive_rating**: Points scored/allowed per 100 possessions. Key advanced metrics for efficiency analysis.
- **assist_percentage/ratio**: Percentage of teammate field goals assisted by the player. Important for playmaking evaluation.
- **defensive_rebound_percentage/rebound_percentage**: Percentage of available rebounds grabbed. Critical for rebounding analysis.
- **turnover_ratio**: Turnovers per 100 possessions. Used in evaluating ball security.
- **usage_percentage/estimated_usage_percentage**: Percentage of team plays used by the player. Important for understanding player role.
- **pie**: Player Impact Estimate. Comprehensive metric for overall impact.
- **technical_foul_count**: Number of technical fouls. Used in discipline analysis.
- **technical_foul_description1/2**: Descriptions of technical fouls. Provides context for technical fouls.

## Common Query Patterns

- Retrieve player performance for a specific game: `SELECT * FROM dwh_f_player_boxscore WHERE game_id = 'XYZ' AND player_id = 'ABC';`
- Compare shooting efficiency across games: `SELECT game_id, field_goals_made, field_goals_attempted FROM dwh_f_player_boxscore WHERE player_id = 'ABC';`
- Analyze team performance contributions: `SELECT team_id, SUM(points) FROM dwh_f_player_boxscore WHERE game_id = 'XYZ' GROUP BY team_id;`
- Evaluate player impact using advanced metrics: `SELECT player_id, pie, offensive_rating, defensive_rating FROM dwh_f_player_boxscore WHERE game_id = 'XYZ';`

## Join Relationships

- **game_id**: Typically joined with a games table to retrieve game details.
- **team_id**: Joined with a teams table for team information.
- **player_id**: Joined with a players table to get player demographics and career stats.