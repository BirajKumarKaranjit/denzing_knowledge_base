---
name: dwh_f_team_boxscore
description: "Use when the query involves analyzing team performance metrics in basketball games. This table captures comprehensive statistics for each team per game, including shooting efficiency, rebounding, assists, turnovers, and advanced metrics like offensive and defensive ratings. It is essential for queries that require detailed breakdowns of team performance by quarters or overtime, and for calculating derived statistics such as pace and usage percentages. Ideal for performance analysis, trend identification, and comparative studies between teams."
tags: [basketball, team performance, analytics, game statistics]
priority: high
fk_to:
  - column: team_id
    ref_table: dwh_d_teams
    ref_column: team_id
  - column: game_id
    ref_table: dwh_d_games
    ref_column: game_id
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

- **id**: Unique identifier for each team boxscore entry. Typically used in SELECT and WHERE clauses for specific record retrieval.
- **game_id**: Identifier linking to the specific game. Commonly used in JOINs with game-related tables.
- **team_id**: Identifier for the team. Used in JOINs with team dimension tables.
- **minutes**: Total minutes played by the team in the game. Generally ranges from 48 to 53+ minutes (including overtime).
- **field_goals_made**: Number of field goals made by the team. Used in SELECT for calculating shooting efficiency.
- **field_goals_attempted**: Number of field goals attempted. Important for calculating field goal percentage.
- **three_pointers_made**: Number of three-point shots made. Used in SELECT for three-point shooting analysis.
- **three_pointers_attempted**: Number of three-point shots attempted. Used to calculate three-point shooting percentage.
- **free_throws_made**: Number of free throws made. Used in SELECT for free throw efficiency analysis.
- **free_throws_attempted**: Number of free throws attempted. Used to calculate free throw percentage.
- **rebounds_offensive**: Number of offensive rebounds. Important for analyzing second-chance opportunities.
- **rebounds_defensive**: Number of defensive rebounds. Used to assess defensive performance.
- **assists**: Number of assists. Indicates team playmaking ability.
- **steals**: Number of steals. Used to evaluate defensive pressure.
- **blocks**: Number of blocks. Indicates defensive presence in the paint.
- **turnovers**: Number of turnovers. Used to assess ball control.
- **fouls_personal**: Number of personal fouls. Important for understanding team discipline.
- **points**: Total points scored by the team. Key metric for performance evaluation.
- **plus_minus_points**: Net point differential when the team is on the court. Used for evaluating team impact.
- **percentage_points_midrange_2pt**: Percentage of points from midrange two-point shots. Used for shot distribution analysis.
- **percentage_points_fastbreak**: Percentage of points from fast breaks. Indicates transition offense efficiency.
- **percentage_points_off_turnovers**: Percentage of points scored off turnovers. Used to assess capitalizing on opponent mistakes.
- **percentage_points_paint**: Percentage of points scored in the paint. Indicates inside scoring effectiveness.
- **percentage_assisted_2pt**: Percentage of two-point field goals assisted. Used to evaluate team playmaking.
- **percentage_unassisted_2pt**: Percentage of two-point field goals unassisted. Indicates individual scoring ability.
- **percentage_assisted_3pt**: Percentage of three-point field goals assisted. Used to assess team ball movement.
- **percentage_unassisted_3pt**: Percentage of three-point field goals unassisted. Indicates individual shooting capability.
- **percentage_assisted_fgm**: Percentage of all field goals assisted. Reflects overall team playmaking.
- **percentage_unassisted_fgm**: Percentage of all field goals unassisted. Indicates reliance on individual scoring.
- **team_turnover_percentage**: Turnovers per 100 possessions. Used to assess team ball security.
- **offensive_rebound_percentage**: Percentage of available offensive rebounds secured. Indicates second-chance opportunities.
- **opp_team_turnover_percentage**: Opponent turnovers per 100 possessions. Used to evaluate defensive pressure.
- **opp_offensive_rebound_percentage**: Opponent's offensive rebound percentage. Used to assess defensive rebounding.
- **estimated_offensive_rating**: Estimated points scored per 100 possessions. Key metric for offensive efficiency.
- **estimated_defensive_rating**: Estimated points allowed per 100 possessions. Key metric for defensive efficiency.
- **defensive_rating**: Actual points allowed per 100 possessions. Used for defensive performance analysis.
- **estimated_net_rating**: Difference between offensive and defensive ratings. Used to evaluate overall team performance.
- **assist_ratio**: Assists per 100 possessions. Indicates team playmaking efficiency.
- **defensive_rebound_percentage**: Percentage of available defensive rebounds secured. Used to assess defensive rebounding.
- **rebound_percentage**: Overall percentage of available rebounds secured. Indicates overall rebounding effectiveness.
- **turnover_ratio**: Turnovers per 100 possessions. Used to evaluate ball control.
- **usage_percentage**: Percentage of team plays used by a player while on the floor. Indicates player involvement.
- **estimated_usage_percentage**: Estimated usage percentage. Used for player involvement analysis.
- **estimated_pace**: Estimated number of possessions per 48 minutes. Used to assess game tempo.
- **pace**: Actual number of possessions per 48 minutes. Used for tempo analysis.
- **possessions**: Total number of possessions in the game. Used in pace and efficiency calculations.
- **pie**: Player Impact Estimate, a measure of a team's overall statistical contribution. Used for performance evaluation.
- **qtr1_points**: Points scored in the first quarter. Used for quarter-by-quarter analysis.
- **qtr2_points**: Points scored in the second quarter. Used for quarter-by-quarter analysis.
- **qtr3_points**: Points scored in the third quarter. Used for quarter-by-quarter analysis.
- **qtr4_points**: Points scored in the fourth quarter. Used for quarter-by-quarter analysis.
- **ot1_points**: Points scored in the first overtime. Used for overtime performance analysis.
- **ot2_points**: Points scored in the second overtime. Used for overtime performance analysis.
- **ot3_points**: Points scored in the third overtime. Used for overtime performance analysis.
- **ot4_points**: Points scored in the fourth overtime. Used for overtime performance analysis.
- **ot5_points**: Points scored in the fifth overtime. Used for overtime performance analysis.
- **ot6_points**: Points scored in the sixth overtime. Used for overtime performance analysis.
- **ot7_points**: Points scored in the seventh overtime. Used for overtime performance analysis.
- **ot8_points**: Points scored in the eighth overtime. Used for overtime performance analysis.
- **ot9_points**: Points scored in the ninth overtime. Used for overtime performance analysis.
- **ot10_points**: Points scored in the tenth overtime. Used for overtime performance analysis.

## Common Query Patterns

- Retrieve team performance metrics for a specific game using `WHERE game_id = 'some_game_id'`.
- Calculate shooting efficiency by selecting `field_goals_made` and `field_goals_attempted` for a team.
- Analyze quarter-by-quarter performance using `qtr1_points`, `qtr2_points`, etc.
- Compare offensive and defensive ratings across games using `estimated_offensive_rating` and `estimated_defensive_rating`.

## Join Relationships

- **game_id**: Typically joined with a game dimension table to retrieve game details.
- **team_id**: Joined with a team dimension table to get team-specific information such as team name and conference