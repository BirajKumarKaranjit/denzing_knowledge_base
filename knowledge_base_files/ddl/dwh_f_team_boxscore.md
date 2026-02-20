---
name: dwh_f_team_boxscore
description: "Use when the query involves analyzing team performance metrics in NBA games, such as field goals, three-pointers, free throws, rebounds, assists, steals, blocks, turnovers, and points. This table is essential for evaluating team efficiency, pace, and scoring distribution across different game segments. It provides detailed insights into team strategies, including fastbreak points, points off turnovers, and paint points, as well as advanced metrics like offensive and defensive ratings, assist ratios, and usage percentages."
tags: [team performance, boxscore, NBA analytics, game metrics]
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
- **game_id**: Identifier linking to the specific game. Used for joining with game-related tables.
- **team_id**: Identifier for the team, used to join with team dimension tables.
- **minutes**: Total minutes played by the team in the game. Usually 48 for regulation games, higher if overtime.
- **field_goals_made/attempted**: Number of field goals made/attempted by the team. Key for calculating shooting efficiency.
- **three_pointers_made/attempted**: Number of three-point shots made/attempted. Important for analyzing team shooting strategy.
- **free_throws_made/attempted**: Number of free throws made/attempted. Indicates team's ability to capitalize on free throw opportunities.
- **rebounds_offensive/defensive**: Number of offensive/defensive rebounds. Critical for assessing team control over the game.
- **assists**: Total assists, reflecting team playmaking ability.
- **steals/blocks**: Defensive metrics indicating team defensive prowess.
- **turnovers**: Number of turnovers, a negative metric affecting team performance.
- **fouls_personal**: Total personal fouls committed by the team.
- **points**: Total points scored by the team, a primary performance indicator.
- **plus_minus_points**: Net point differential when the team is on the court.
- **percentage_points_midrange_2pt/fastbreak/off_turnovers/paint**: Distribution of points from various play types, indicating team scoring strategy.
- **percentage_assisted/unassisted_2pt/3pt/fgm**: Proportion of field goals that are assisted/unassisted, reflecting team play style.
- **team_turnover_percentage**: Turnovers per 100 possessions, a measure of ball security.
- **offensive_rebound_percentage**: Percentage of available offensive rebounds secured by the team.
- **opp_team_turnover_percentage/opp_offensive_rebound_percentage**: Opponent's turnover and offensive rebound percentages, used for defensive analysis.
- **estimated_offensive/defensive_rating**: Points scored/allowed per 100 possessions, key efficiency metrics.
- **defensive_rating**: Actual defensive performance metric.
- **estimated_net_rating**: Difference between estimated offensive and defensive ratings.
- **assist_ratio**: Assists per 100 possessions, indicating team passing efficiency.
- **defensive_rebound_percentage/rebound_percentage**: Team's ability to secure defensive rebounds and overall rebounds.
- **turnover_ratio**: Turnovers per 100 possessions, another measure of ball security.
- **usage_percentage/estimated_usage_percentage**: Percentage of team plays used by a player while on the court.
- **estimated_pace/pace**: Number of possessions per 48 minutes, indicating game speed.
- **possessions**: Total possessions in the game.
- **pie**: Player Impact Estimate, a measure of a team's overall statistical contribution.
- **qtr1_points/qtr2_points/qtr3_points/qtr4_points**: Points scored by the team in each quarter.
- **ot1_points to ot10_points**: Points scored in each overtime period, if applicable.

## Common Query Patterns

- Calculate team shooting efficiency by comparing field goals made to attempted.
- Analyze team performance trends over a season by joining with game and team tables using `game_id` and `team_id`.
- Evaluate team defensive capabilities by examining steals, blocks, and defensive ratings.
- Determine scoring distribution by analyzing points from fastbreaks, turnovers, and paint.

## Join Relationships

- **game_id**: Join with game dimension tables to retrieve game-specific details.
- **team_id**: Join with team dimension tables to get team metadata and historical performance.
- Often used in conjunction with player boxscore tables to compare individual and team performances.