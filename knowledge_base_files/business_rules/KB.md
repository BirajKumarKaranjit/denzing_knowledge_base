---
name: business_rules
description: "NBA domain metric definitions, KPI formulas, and basketball statistics calculations."
---

# Business Rules & Metric Definitions

## Core Counting Stats
- **Points (PTS)**: Direct from box_scores.points
- **Rebounds (REB)**: offensive_rebounds + defensive_rebounds
- **Assists (AST)**: Direct from box_scores.assists

## Advanced Metrics
- **True Shooting % (TS%)**: points / (2 * (field_goal_attempts + 0.44 * free_throw_attempts))
- **Effective FG% (eFG%)**: (field_goals_made + 0.5 * three_pointers_made) / field_goal_attempts
- **Player Efficiency Rating (PER)**: Use the standard Hollinger formula (complex — prefer pre-computed columns if available)

## Game Filters
- Regular season only: WHERE game_type = 'regular'
- Playoffs only: WHERE game_type = 'playoff'
- Default to regular season unless user specifies playoffs

## Player Activity
- Active players: WHERE is_active = TRUE in players table
- Minimum games filter for statistical relevance: typically >= 20 games per season
