---
name: dwh_f_team_boxscore
description: "Use when the query involves analyzing team performance metrics in NBA games. This table provides comprehensive statistics for each team per game, including shooting efficiency, rebounding, assists, turnovers, and advanced metrics like offensive and defensive ratings. Ideal for queries that require breakdowns of team performance by quarters or overtime periods, or when calculating team-specific metrics such as pace, usage percentage, and net rating. Suitable for detailed game analysis, team comparisons, and performance trend analysis over a season."
tags: [team performance, game statistics, NBA analytics]
priority: high
---

# DDL

```sql
CREATE TABLE dwh_f_team_boxscore (
    id text,
    game_id text,
    team_id text,
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
    percentage_points_midrange_2pt numeric,
    percentage_points_fastbreak numeric,
    percentage_points_off_turnovers numeric,
    percentage_points_paint numeric,
    percentage_assisted_2pt numeric,
    percentage_unassisted_2pt numeric,
    percentage_assisted_3pt numeric,
    percentage_unassisted_3pt numeric,
    percentage_assisted_fgm numeric,
    percentage_unassisted_fgm numeric,
    team_turnover_percentage numeric,
    offensive_rebound_percentage numeric,
    opp_team_turnover_percentage numeric,
    opp_offensive_rebound_percentage numeric,
    estimated_offensive_rating numeric,
    estimated_defensive_rating numeric,
    defensive_rating numeric,
    estimated_net_rating numeric,
    assist_ratio numeric,
    defensive_rebound_percentage numeric,
    rebound_percentage numeric,
    turnover_ratio numeric,
    usage_percentage numeric,
    estimated_usage_percentage numeric,
    estimated_pace numeric,
    pace numeric,
    possessions numeric,
    pie numeric,
    qtr1_points numeric,
    qtr2_points numeric,
    qtr3_points numeric,
    qtr4_points numeric,
    ot1_points numeric,
    ot2_points numeric,
    ot3_points numeric,
    ot4_points numeric,
    ot5_points numeric,
    ot6_points numeric,
    ot7_points numeric,
    ot8_points numeric,
    ot9_points numeric,
    ot10_points numeric
);
```

## Column Semantics

- **id**: Unique identifier for each team boxscore entry. Typically used in SELECT and WHERE clauses.
- **game_id**: Identifier for the game, linking to game details. Essential for JOINs with game tables.
- **team_id**: Identifier for the team, used to JOIN with team dimension tables.
- **minutes**: Total minutes played by the team in the game. Usually 48 for regulation games.
- **field_goals_made/attempted**: Number of field goals made/attempted by the team. Used to calculate shooting percentages.
- **three_pointers_made/attempted**: Number of three-point shots made/attempted. Key for analyzing team shooting efficiency.
- **free_throws_made/attempted**: Number of free throws made/attempted. Important for assessing team scoring efficiency.
- **rebounds_offensive/defensive**: Number of offensive/defensive rebounds. Critical for evaluating team rebounding strength.
- **assists**: Total assists made by the team. Reflects team playmaking ability.
- **steals/blocks**: Defensive metrics indicating team defensive prowess.
- **turnovers**: Number of turnovers committed by the team. High values indicate potential issues with ball control.
- **fouls_personal**: Number of personal fouls committed. Used to assess team discipline.
- **points**: Total points scored by the team. Central to any scoring analysis.
- **plus_minus_points**: Net point differential when the team is on the court. Used to gauge overall team impact.
- **percentage_points_midrange_2pt/fastbreak/off_turnovers/paint**: Breakdown of scoring sources. Useful for strategic analysis.
- **percentage_assisted/unassisted_2pt/3pt/fgm**: Indicates reliance on assisted vs. unassisted scoring.
- **team_turnover_percentage**: Turnovers per 100 possessions. Key for efficiency analysis.
- **offensive/defensive_rebound_percentage**: Percentage of available rebounds secured. Important for rebounding analysis.
- **estimated_offensive/defensive/net_rating**: Advanced metrics estimating points scored/allowed per 100 possessions.
- **assist_ratio**: Assists per 100 possessions. Reflects team passing efficiency.
- **turnover_ratio**: Turnovers per 100 possessions. Used to evaluate ball security.
- **usage_percentage**: Estimate of team possessions used by a player. Important for player impact analysis.
- **pace**: Estimate of possessions per 48 minutes. Used to assess game tempo.
- **possessions**: Total possessions in the game. Fundamental for calculating advanced metrics.
- **pie**: Player Impact Estimate, reflecting overall team performance.
- **qtr1_points, qtr2_points, qtr3_points, qtr4_points**: Points scored in each quarter. Useful for analyzing scoring trends.
- **ot1_points to ot10_points**: Points scored in overtime periods. Relevant for games extending beyond regulation.

## Common Query Patterns

- Retrieve team performance metrics for a specific game: `SELECT * FROM dwh_f_team_boxscore WHERE game_id = '20231025'`
- Compare team performance across multiple games: `SELECT team_id, AVG(points) FROM dwh_f_team_boxscore WHERE game_id IN ('20231025', '20231026') GROUP BY team_id`
- Analyze scoring trends by quarter: `SELECT game_id, qtr1_points, qtr2_points, qtr3_points, qtr4_points FROM dwh_f_team_boxscore WHERE team_id = 'LAL'`

## Join Relationships

- **game_id**: Joins with game tables to fetch game-specific details.
- **team_id**: Joins with team dimension tables to retrieve team metadata such as team name and conference.
- Typically joined with player boxscore tables to aggregate player contributions to team performance.