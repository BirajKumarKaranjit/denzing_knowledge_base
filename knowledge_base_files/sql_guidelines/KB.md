---
name: sql_guidelines
description: "Query patterns, join conventions, and SQL best practices for the NBA analytics database."
---

# SQL Guidelines

## General Conventions
- Always use table aliases for readability (e.g., p for players, g for games)
- Use DATE_TRUNC('season', game_date) for season-level aggregations
- Exclude preseason games unless explicitly requested (filter by game_type = 'regular')

## Common Join Patterns
- box_scores JOIN players ON box_scores.player_id = players.id
- box_scores JOIN games ON box_scores.game_id = games.id
- games JOIN teams ON games.home_team_id = teams.id OR games.away_team_id = teams.id

## Aggregation Rules
- Per-game stats: always divide by COUNT(DISTINCT game_id)
- Season totals: GROUP BY player_id, season_year
- Use NULLIF(denominator, 0) for all division operations

## Date Handling
- game_date is TIMESTAMP — use DATE_TRUNC or CAST to DATE for day-level queries
- Season year convention: 2023 means the 2023-24 season
- Never use CURRENT_DATE — filter dynamically on MAX(game_date) in the data

## Performance
- Filter by season_year before joining (reduces scan size significantly)
- Use player_id / team_id for joins, never player_name (non-indexed)
