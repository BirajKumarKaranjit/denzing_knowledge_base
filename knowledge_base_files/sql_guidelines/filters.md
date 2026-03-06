---
name: filters
description: "Use for every query — governs WHERE clause decisions: ILIKE name matching, game type defaulting, season scope (current/previous/career/rookie), last game resolution, last N games, streak calculation, threshold filters, foul outs, month filters, and player team identity. The two most critical defaults: always regular season unless stated, always current season for present-tense queries."
tags: [filters, WHERE, ILIKE, player name, team name, season scope, current season, career, last game, last N games, streak, gaps-and-islands, threshold, regular season, playoffs, pb.team_id]
priority: critical
---

# SQL Filtering Guidelines

---

## RULE 1 — Name Matching: Always ILIKE '%name%', Never =

Player names include suffixes (Jr., II, III) and accents (Jokić). Any `=` match silently returns zero rows when the stored value differs.

```sql
-- CORRECT
WHERE p.full_name ILIKE '%Jimmy Butler%'   -- matches "Jimmy Butler III"
WHERE p.full_name ILIKE '%Gary Payton%'    -- matches Gary Payton and Gary Payton II
WHERE p.full_name ILIKE '%Nikola Jokic%'   -- matches "Nikola Jokić"
WHERE t.full_name ILIKE '%Timberwolves%'

-- WRONG
WHERE p.full_name = 'Jimmy Butler'
```

When ILIKE matches multiple players (e.g., both Gary Paytons), return all matches — do not narrow arbitrarily.

Apply to all string columns: player names, team names, game_type, conference, position.

---

## RULE 2 — Categorical Fields: ILIKE, Never Strict Equality

```sql
WHERE g.game_type ILIKE '%Regular Season%'
WHERE g.game_type ILIKE '%playoff%'
WHERE t.conference ILIKE '%East%'   -- stored as 'East', not 'Eastern Conference'
WHERE t.conference ILIKE '%West%'   -- stored as 'West', not 'Western Conference'
```

---

## RULE 3 — Season Scope

### Present-tense → Current Season (REQUIRED)

Trigger phrases: "this season", "this year", "currently", "leads", "averaging", "who is the best", "how is X doing", "right now".

```sql
AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
AND g.game_type ILIKE '%Regular Season%'
```

Never hardcode `season_year = '2024'` — always use `MAX(season_year)` dynamically.

### "Last season" / "last year" → Previous Season

```sql
AND g.season_year = (
    SELECT MAX(season_year) FROM dwh_d_games
    WHERE season_year < (SELECT MAX(season_year) FROM dwh_d_games)
      AND game_type ILIKE '%Regular Season%'
)
```

### Career / All-time / Ever → No Season Filter

Trigger phrases: "career", "all-time", "ever", "in his career", "total", "career high", "how many total". Omit `season_year` and `game_type` filters entirely unless user says "regular season career".

### Rookie Season → Dynamic MIN, Never Hardcoded

```sql
AND g.season_year = (
    SELECT MIN(season_year) FROM dwh_f_player_team_seasons
    WHERE player_id = (SELECT player_id FROM dwh_d_players WHERE full_name ILIKE '%Bronny James%')
)
```

### Default When Nothing Is Specified

Regular season + current season. Both filters always applied.

---

## RULE 4 — Last Game: ORDER BY game_date DESC LIMIT 1, Never CURRENT_DATE

```sql
-- Player's last game
WITH last_game AS (
    SELECT pb.game_id FROM dwh_f_player_boxscore pb
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    WHERE p.full_name ILIKE '%Kevin Durant%'
    ORDER BY g.game_date DESC LIMIT 1
)

-- Team's last game
WHERE g.game_date = (
    SELECT MAX(g2.game_date) FROM dwh_d_games g2
    JOIN dwh_d_teams t ON g2.home_team_id = t.team_id OR g2.visitor_team_id = t.team_id
    WHERE t.full_name ILIKE '%Miami Heat%'
)

-- WRONG
WHERE g.game_date = CURRENT_DATE - 1
```

---

## RULE 5 — Last N Games: ORDER BY game_date DESC LIMIT N

Never use `ORDER BY game_id DESC` — game_id is not chronological.

```sql
WITH recent AS (
    SELECT pb.game_id FROM dwh_f_player_boxscore pb
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    WHERE p.full_name ILIKE '%LeBron James%' AND g.game_type ILIKE '%Regular Season%'
    ORDER BY g.game_date DESC LIMIT 5
)
SELECT p.full_name, AVG(pb.points) AS avg_pts, AVG(pb.assists) AS avg_ast
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN recent r ON pb.game_id = r.game_id
GROUP BY p.full_name;
```

---

## RULE 6 — Streaks: Full Game Sequence + Gaps-and-Islands, Never Pre-Filter

Pre-filtering qualifying games (e.g., `WHERE points >= 30`) removes the gaps between them, making non-consecutive games appear consecutive and inflating every streak.

```sql
-- CORRECT: keep ALL games, flag, then find streaks
WITH all_games AS (
    SELECT pb.player_id, g.game_date,
        ROW_NUMBER() OVER (PARTITION BY pb.player_id ORDER BY g.game_date) AS rn,
        CASE WHEN pb.points >= 30 THEN 1 ELSE 0 END AS qualifies
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Stephen Curry%' AND g.game_type ILIKE '%Regular Season%'
),
streaks AS (
    SELECT *, rn - ROW_NUMBER() OVER (PARTITION BY player_id, qualifies ORDER BY game_date) AS grp
    FROM all_games
)
SELECT MIN(game_date) AS start, MAX(game_date) AS end, COUNT(*) AS length
FROM streaks WHERE qualifies = 1
GROUP BY player_id, grp ORDER BY length DESC LIMIT 1;

-- WRONG: filtering first inflates streaks
WHERE player_id = :id AND points >= 30
```

Applies to all streak types: scoring, plus-minus, double-doubles, steals, assists, three-pointers, zero-turnover — any consecutive-game threshold.

---

## RULE 7 — Threshold Filters: Strict > for Denominators

```sql
-- CORRECT: requires at least 1 attempt
WHERE pb.field_goals_attempted > 0 AND pb.field_goals_made = pb.field_goals_attempted

-- WRONG: 0/0 passes as "perfect"
WHERE pb.field_goals_attempted >= 0
```

---

## RULE 8 — Month / Date-Range Filters

```sql
-- This month
WHERE EXTRACT(MONTH FROM g.game_date) = EXTRACT(MONTH FROM CURRENT_DATE)
  AND EXTRACT(YEAR  FROM g.game_date) = EXTRACT(YEAR  FROM CURRENT_DATE)

-- Specific month
WHERE DATE_TRUNC('month', g.game_date) = DATE '2025-01-01'
```

---

## RULE 9 — Player Team Identity: pb.team_id Only

When filtering player stats to a team, use `pb.team_id`. Joining the game table on both home/visitor teams then pulling all player stats includes the opponent's players.

```sql
-- CORRECT
JOIN dwh_d_teams t ON pb.team_id = t.team_id
WHERE t.full_name ILIKE '%Timberwolves%'

-- WRONG: includes opponent players
JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
WHERE t.full_name ILIKE '%Timberwolves%'
```

---

## RULE 10 — No LIMIT Inside Analytical CTEs

```sql
WITH data AS (SELECT ... LIMIT 1000)  -- WRONG: silently drops data mid-analysis
SELECT ... FROM cte ORDER BY pts DESC LIMIT 10;  -- CORRECT: LIMIT on final output only
```

---

## RULE 11 — NULL Handling

```sql
WHERE school IS NOT NULL   -- CORRECT
WHERE school = NULL        -- WRONG
```

---

## RULE 12 — STRICTLY Always Apply These Two Filters by Default
***Users never explicitly ask for "regular season" or "this year" — they just ask about basketball. Always apply both filters unless the query contains a specific override signal.***
sql-- These two lines go in every query by default
    AND g.game_type ILIKE '%Regular Season%'
    AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
Override signals — when present, replace the defaults:
Signal Override:
- "playoffs", "postseason"Replace game_type with ILIKE '%playoff%'"career", "all-time", "ever", "total", 
- "career high"Remove both filters entirely"last season",
- "last year"Replace season filter with previous season subquery 
- specific year mentioned ("in 2022")Replace season filter with that year
- "last game","most recent game", "last night"Remove season filter; use ORDER BY game_date DESC LIMIT 1
- implied recency — "what did X score", "how did X play", "what is X's stat line", "how many points did X score" (no time context given)Use ORDER BY g.game_date DESC LIMIT 1 — user wants the latest game, not a season aggregate

## Concrete examples

Example A — implied recency (user asks a single-game stat or score):
User: What is the score of LeBron James?
Interpretation: implied recency → last game stat line (do not apply season aggregate).

-- CORRECT: implied recency → last game: remove default filters, order by date
SELECT p.full_name, g.game_date,
    ht.full_name AS home_team, vt.full_name AS visitor_team,
    g.home_score, g.visitor_score,
    pb.points, pb.assists,
    pb.rebounds_offensive + pb.rebounds_defensive AS total_rebounds
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
JOIN dwh_d_teams ht  ON g.home_team_id    = ht.team_id
JOIN dwh_d_teams vt  ON g.visitor_team_id = vt.team_id
WHERE p.full_name ILIKE '%LeBron James%'
ORDER BY g.game_date DESC
LIMIT 1;

-- WRONG: no time context handling → DO NOT produce a season aggregate for this implicit-last-game question
SELECT p.full_name, SUM(pb.points) AS total_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
WHERE p.full_name ILIKE '%LeBron James%'
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name;

Example B — explicit season request:
User: How many points did LeBron score this season?
Interpretation: present-tense → apply defaults (latest regular season + regular-season game_type), aggregate over that season.

-- CORRECT: present-tense → default filters apply
SELECT p.full_name, SUM(pb.points) AS total_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g ON pb.game_id = g.game_id
WHERE p.full_name ILIKE '%LeBron James%'
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name;

Example C — playoffs + specific year:
User: How many points did X score in the 2022 playoffs?
Interpretation: season = 2022, game_type = playoffs (both applied).

AND g.game_type ILIKE '%playoff%'
AND g.season_year = '2022'
---
## Quick-Reference

| Situation | Filter |
|---|---|
| Default game type | `game_type ILIKE '%Regular Season%'` |
| Default season (present-tense) | `season_year = (SELECT MAX(season_year) ...)` |
| Last season | `MAX(season_year) WHERE season_year < MAX(...)` |
| Career / all-time | No `season_year` filter |
| Rookie season | `MIN(season_year) FROM dwh_f_player_team_seasons` |
| Last game | `ORDER BY game_date DESC LIMIT 1` |
| Last N games | `ORDER BY game_date DESC LIMIT N` |
| Conference | `'East'` / `'West'` — not full names |
| Player name | `ILIKE '%name%'` — never `=` |
| Player's team | `pb.team_id` — never inferred from game table |