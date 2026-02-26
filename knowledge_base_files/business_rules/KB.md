---
name: business_rules
description: "Domain knowledge for this analytics system: NBA metric definitions, KPI formulas, basketball statistics calculations, and Denzing platform information. Use when the query involves derived stats, efficiency ratings, business logic, or questions about the agent and platform itself."
---

# Business Rules & Domain Knowledge

This section contains domain-specific knowledge the agent uses to interpret queries correctly
and generate accurate responses. It covers two areas: NBA analytics business logic and
Denzing platform context.

## Contents

- **KB.md** (this file) — Core NBA metric definitions and game filter rules
- **project_information.md** — Denzing platform overview, capabilities, integrations, and NBA agent context

---

## NBA Metric Definitions

### Core Counting Stats
- **Points (PTS)**: Direct from `dwh_f_player_boxscore.points`
- **Rebounds (REB)**: `rebounds_offensive + rebounds_defensive`
- **Assists (AST)**: Direct from `dwh_f_player_boxscore.assists`
- **Steals (STL)**: Direct from `dwh_f_player_boxscore.steals`
- **Blocks (BLK)**: Direct from `dwh_f_player_boxscore.blocks`
- **Turnovers (TOV)**: Direct from `dwh_f_player_boxscore.turnovers`

### Advanced Metrics
- **True Shooting % (TS%)**: `points / (2 * (field_goals_attempted + 0.44 * free_throws_attempted))`
- **Effective FG% (eFG%)**: `(field_goals_made + 0.5 * three_pointers_made) / field_goals_attempted`
- **Player Efficiency Rating (PER)**: Use the standard Hollinger formula — prefer pre-computed columns if available in the schema.
- **Usage Rate**: `(field_goals_attempted + 0.44 * free_throws_attempted + turnovers) / team_possessions`

### Per-Game Averages
Always divide season totals by `COUNT(DISTINCT game_id)` — never by a fixed number.
Use `NULLIF(denominator, 0)` for all division to avoid divide-by-zero errors.

---

## Game Filter Rules

- **Regular season only**: `WHERE game_type = 'regular'`
- **Playoffs only**: `WHERE game_type = 'playoff'`
- **Default**: Regular season unless the user explicitly requests playoffs.

---

## Player Activity Rules

- **Active players**: `WHERE is_active = TRUE` in `dwh_d_players`
- **Minimum games threshold**: Apply `HAVING COUNT(DISTINCT game_id) >= 20` for statistical relevance unless the user specifies otherwise.
