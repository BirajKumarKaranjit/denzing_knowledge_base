---
name: joins
description: "Use when the query requires combining data from multiple tables. Covers JOIN patterns between player stats, game results, team data, box scores, and dimension tables. Choose this for any query that links players to teams, games to scores, or box score data to player/team dimensions."
tags: [joins, multi-table, foreign-key, relationships]
priority: high
---

# JOIN Patterns for NBA Analytics

## Standard Join Conventions

- Always use explicit `JOIN ... ON ...` syntax — never implicit comma joins.
- Use short, consistent table aliases: `p` for players, `g` for games, `bs` for box scores, `t` for teams.
- Prefer `INNER JOIN` unless the query explicitly requires unmatched rows (use `LEFT JOIN` then).

## Core Join Patterns

### Player Box Score → Player Dimension
```sql
SELECT p.full_name, bs.points, bs.assists, bs.rebounds_offensive + bs.rebounds_defensive AS total_rebounds
FROM dwh_f_player_boxscore bs
JOIN dwh_d_players p ON bs.player_id = p.player_id
WHERE bs.game_id = :game_id;
```

### Player Box Score → Game Dimension
```sql
SELECT g.game_date, g.season_year, bs.points
FROM dwh_f_player_boxscore bs
JOIN dwh_d_games g ON bs.game_id = g.game_id
WHERE bs.player_id = :player_id;
```

### Team Box Score → Team Dimension
```sql
SELECT t.full_name AS team_name, tbs.points AS team_points
FROM dwh_f_team_boxscore tbs
JOIN dwh_d_teams t ON tbs.team_id = t.team_id
WHERE tbs.game_id = :game_id;
```

### Game → Home/Away Teams (double join)
```sql
SELECT g.game_date,
       ht.full_name AS home_team,
       at.full_name AS away_team,
       g.home_score,
       g.visitor_score
FROM dwh_d_games g
JOIN dwh_d_teams ht ON g.home_team_id = ht.team_id
JOIN dwh_d_teams at ON g.visitor_team_id = at.team_id;
```

### Player Awards → Player Dimension
```sql
SELECT p.full_name, a.description AS award, a.season
FROM dwh_f_player_awards a
JOIN dwh_d_players p ON a.player_id = p.player_id;
```

### Player Tracking → Player + Game
```sql
SELECT p.full_name, g.game_date, pt.speed, pt.distance
FROM dwh_f_player_tracking pt
JOIN dwh_d_players p ON pt.player_id = p.player_id
JOIN dwh_d_games g ON pt.game_id = g.game_id;
```

## Gotchas

- `dwh_d_games` uses `home_team_id` and `visitor_team_id` — join to `dwh_d_teams` TWICE with different aliases when both team names are needed.
- `dwh_f_player_team_seasons` links players to their team per season and game type — use this for season-level player-team association, not `dwh_f_player_boxscore`.
- Never join on `full_name` or `player_slug` — always use `player_id` / `team_id` (indexed, unique, stable).

