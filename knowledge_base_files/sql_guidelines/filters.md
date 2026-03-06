---
name: filters
description: "Use when the query involves any WHERE clause condition — player name lookup, team name lookup, game type scoping, season scoping (current, previous, career, rookie, specific), last game resolution, last N games, streak calculation, threshold filters, foul outs, month filters, or NULL handling. Contains the primary decision rules that determine whether to apply a current season filter, career/all-time filter, previous season filter, or no filter. Must be consulted for every query involving a player or team name, any time period reference, or any performance threshold."
tags: [filters, WHERE clause, ILIKE, player name, team name, season scoping, current season, career, all-time, last game, last N games, streak, gaps-and-islands, threshold, perfect shooting, foul outs, month filter, NULL, regular season, playoffs, back-to-back]
priority: critical
---

# SQL Filtering Guidelines

---

## RULE 1 — Entity Name Matching: Always ILIKE with Wildcards, Never =

Player names in the database include suffixes (Jr., II, III) and accents (Jokić, Dončić). **Never use `=` for any name lookup.** A single `=` silently returns zero rows when the stored value has any suffix or accent variation.

```sql
-- CORRECT: catches "Jimmy Butler III", "Gary Payton II", "Nikola Jokić"
WHERE p.full_name ILIKE '%Jimmy Butler%'
WHERE p.full_name ILIKE '%Gary Payton%'
WHERE p.full_name ILIKE '%Nikola Jokic%'   -- accent variation still matches
WHERE t.full_name ILIKE '%Timberwolves%'

-- WRONG: silently returns 0 rows if stored name has any suffix or accent
WHERE p.full_name = 'Jimmy Butler'          -- misses "Jimmy Butler III"
WHERE p.full_name = 'Gary Payton'           -- misses "Gary Payton II"
```

Apply ILIKE to: player full_name, team full_name, position, conference, game_type, and any other string column.

**Confirmed failures from benchmarking:** Rows 46, 51, 61, 99, 116 — all failed because `=` was used instead of `ILIKE` for player names with suffixes.

---

## RULE 2 — Ambiguous Names: Return All Matches

When ILIKE matches multiple players (e.g., Gary Payton and Gary Payton II), return all matches. Do not silently narrow down. The user benefits from seeing both.

```sql
-- CORRECT: returns both Gary Payton and Gary Payton II
WHERE p.full_name ILIKE '%Gary Payton%'

-- WRONG: silently excludes one player
WHERE p.full_name = 'Gary Payton II'
```

**Confirmed success from benchmarking:** Row 75 — query correctly returned both Gary Paytons and explained them.

---

## RULE 3 — Categorical Fields: Always ILIKE, Never Strict Equality

Stored values for game_type, conference, and status vary in casing and phrasing across the database.

```sql
-- CORRECT
WHERE g.game_type ILIKE '%Regular Season%'
WHERE g.game_type ILIKE '%playoff%'
WHERE t.conference ILIKE '%East%'
WHERE t.conference ILIKE '%West%'

-- WRONG: fails if stored value uses different casing or phrasing
WHERE g.game_type = 'Playoffs'
WHERE t.conference = 'Eastern Conference'   -- stored as 'East', not full name
WHERE t.conference = 'Western Conference'   -- stored as 'West', not full name
```

**Known stored values:** `t.conference` stores `'East'` and `'West'` — never the full conference names.

---

## RULE 4 — Game Type Default: Regular Season Unless the User Specifies Otherwise

When the user does not mention game type, always default to regular season. Never include preseason. Never mix regular + playoffs unless the user says "career", "all-time", or "playoffs".

```sql
-- Include in every query unless user specifies otherwise
AND g.game_type ILIKE '%Regular Season%'
```

---

## RULE 5 — Season Scope: Match Exactly What the User Is Asking

This is the most critical filter decision. Use the table below to choose the correct season scope.

### Season Scope Decision Table

| User phrasing | Season filter to apply |
|---|---|
| "this season", "this year", "currently", "leads", "averaging", "how is X doing", "who is the best" | **Current season** — `g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')` |
| "last season", "last year", "previous season" | **Previous season** — see Rule 5b |
| "in 2023-24", "in the 2022 season", specific year named | **Specific season** — `g.season_year = '2023'` |
| "career", "all-time", "ever", "in his career", "total", "historically", "how many total" | **No season filter** — omit season_year condition entirely |
| "rookie season", "as a rookie" | **Rookie season dynamic** — see Rule 5d |
| "last game", "most recent", "last night" | **Last game** — `ORDER BY g.game_date DESC LIMIT 1` — see Rule 6 |
| "last 5 games", "last 10 games" | **Last N games** — `ORDER BY g.game_date DESC LIMIT N` — see Rule 7 |
| "this month", "in January" | **Month filter** — see Rule 9 |
| "playoffs" (explicit) | `g.game_type ILIKE '%playoff%'` — no regular season default |
| unspecified (default) | Current season + Regular season |

---

### Rule 5a — Current Season Filter (Present-Tense Queries)

**Failing to apply this causes retired players to appear as current leaders — a critical failure.**

```sql
-- CORRECT: always use MAX subquery, never hardcode a season year
AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
AND g.game_type ILIKE '%Regular Season%'

-- WRONG: hardcoded year breaks when season advances
AND g.season_year = '2024'

-- WRONG: no season filter returns all-time data including retired players
-- (simply omitting season_year on a present-tense query)
```

**Confirmed successes:** Rows 13, 36, 37, 50, 53, 64, 78, 87, 98, 103, 107, 113, 123, 125, 132, 138, 150, 153, 165, 167, 172, 197.

---

### Rule 5b — Previous Season Filter

```sql
AND g.season_year = (
    SELECT MAX(season_year) FROM dwh_d_games
    WHERE season_year < (SELECT MAX(season_year) FROM dwh_d_games)
      AND game_type ILIKE '%Regular Season%'
)
```

**Confirmed successes:** Rows 145, 157, 169, 186, 187.

---

### Rule 5c — Career / All-Time: No Season Filter

When the user asks "in his career", "all-time", "how many total", "career high", "ever" — omit the season_year filter entirely. Include all game types unless user says "regular season career".

```sql
-- Career total — NO season filter
SELECT p.full_name, SUM(pb.points) AS career_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
WHERE p.full_name ILIKE '%LeBron James%'
GROUP BY p.full_name;
```

**Confirmed successes:** Rows 2, 17, 19, 27, 44, 52, 54, 55, 56, 63, 75, 92, 93, 94, 97, 108, 110, 135, 139, 142, 146, 147, 180, 188, 191, 207.

---

### Rule 5d — Rookie Season: Dynamic MIN(season_year), Never Hardcoded

When a user asks about a player as a rookie, or asks to compare rookies, determine the rookie season dynamically from `dwh_f_player_team_seasons`. **Never hardcode a season year.** If the player was a rookie in a prior season, do not apply the current season filter — this causes the query to find no data for them.

```sql
AND g.season_year = (
    SELECT MIN(season_year) FROM dwh_f_player_team_seasons
    WHERE player_id = (SELECT player_id FROM dwh_d_players WHERE full_name ILIKE '%Bronny James%')
)
```

**Confirmed failure fixed:** Rows 32, 33 — were failing because the current season filter was applied instead of MIN(season_year). **Confirmed successes:** Rows 58, 59 — correctly used rookie season lookup.

---

## RULE 6 — Last Game Resolution: ORDER BY game_date DESC, Never CURRENT_DATE

"Last night", "most recent game", "last game", "what did X score last game" — always resolve dynamically from the data. The database may not have games from today or yesterday.

```sql
-- CORRECT: player's most recent game
WITH last_game AS (
    SELECT pb.game_id
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_games g ON pb.game_id = g.game_id
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    WHERE p.full_name ILIKE '%Kevin Durant%'
    ORDER BY g.game_date DESC
    LIMIT 1
)
SELECT p.full_name, g.game_date, pb.points, pb.assists,
       pb.rebounds_offensive + pb.rebounds_defensive AS total_rebounds
FROM dwh_f_player_boxscore pb
JOIN dwh_d_games g ON pb.game_id = g.game_id
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN last_game lg ON pb.game_id = lg.game_id
WHERE p.full_name ILIKE '%Kevin Durant%';

-- CORRECT: team's most recent game score
SELECT g.game_date,
    ht.full_name AS home_team, g.home_score,
    vt.full_name AS visitor_team, g.visitor_score
FROM dwh_d_games g
JOIN dwh_d_teams ht ON g.home_team_id = ht.team_id
JOIN dwh_d_teams vt ON g.visitor_team_id = vt.team_id
JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
WHERE t.full_name ILIKE '%Miami Heat%'
ORDER BY g.game_date DESC
LIMIT 1;

-- WRONG: fails if no game was played yesterday
WHERE g.game_date = CURRENT_DATE - 1
```

**Confirmed failure fixed:** Row 62. **Confirmed successes:** Rows 14, 20, 43, 44, 65, 67, 80, 95, 106, 181, 195.

---

## RULE 7 — Last N Games: ORDER BY game_date DESC with LIMIT N

"Last 5 games", "last 10 games", "past 10 games" — always order by `game_date DESC`. **Never use `game_id DESC`** — game_id ordering is not guaranteed to be chronological.

```sql
-- Player's last 5 games averages
WITH recent AS (
    SELECT pb.game_id
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_games g ON pb.game_id = g.game_id
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    WHERE p.full_name ILIKE '%LeBron James%'
      AND g.game_type ILIKE '%Regular Season%'
    ORDER BY g.game_date DESC
    LIMIT 5
)
SELECT p.full_name,
    AVG(pb.points)  AS avg_points,
    AVG(pb.assists) AS avg_assists,
    AVG(pb.rebounds_offensive + pb.rebounds_defensive) AS avg_rebounds
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN recent r ON pb.game_id = r.game_id
GROUP BY p.full_name;

-- Team's last 10 games
WITH team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Milwaukee Bucks%'),
last10 AS (
    SELECT g.game_id, g.home_team_id, g.visitor_team_id, g.home_score, g.visitor_score
    FROM dwh_d_games g, team t
    WHERE (g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id)
      AND g.game_type ILIKE '%Regular Season%'
    ORDER BY g.game_date DESC LIMIT 10
)
SELECT
    SUM(CASE WHEN (l.home_team_id = t.team_id AND l.home_score > l.visitor_score)
              OR (l.visitor_team_id = t.team_id AND l.visitor_score > l.home_score)
             THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN (l.home_team_id = t.team_id AND l.home_score < l.visitor_score)
              OR (l.visitor_team_id = t.team_id AND l.visitor_score < l.home_score)
             THEN 1 ELSE 0 END) AS losses
FROM last10 l, team t;
```

**Confirmed successes:** Rows 47, 118, 203.

---

## RULE 8 — Streak Calculation: Never Pre-Filter, Always Use Full Game Sequence

**The most common and most damaging streak bug.** Pre-filtering rows (e.g., WHERE points >= 30) removes the gaps between qualifying games, making any two qualifying games appear consecutive. This inflates every streak result.

**The correct pattern:** keep ALL games in sequence, flag each game as qualifying or not, then use gaps-and-islands to find streaks.

```sql
-- CORRECT: full sequence + flag + gaps-and-islands
WITH all_games AS (
    SELECT
        pb.player_id,
        g.game_date,
        pb.points,
        ROW_NUMBER() OVER (PARTITION BY pb.player_id ORDER BY g.game_date) AS rn,
        CASE WHEN pb.points >= 30 THEN 1 ELSE 0 END AS qualifies
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g ON pb.game_id = g.game_id
    WHERE p.full_name ILIKE '%Stephen Curry%'
      AND g.game_type ILIKE '%Regular Season%'
),
streaks AS (
    SELECT *,
        rn - ROW_NUMBER() OVER (
            PARTITION BY player_id, qualifies ORDER BY game_date
        ) AS grp
    FROM all_games
)
SELECT MIN(game_date) AS streak_start,
       MAX(game_date) AS streak_end,
       COUNT(*) AS streak_length
FROM streaks
WHERE qualifies = 1
GROUP BY player_id, grp
ORDER BY streak_length DESC
LIMIT 1;

-- WRONG: filters first — removes gaps — inflates streaks
SELECT * FROM dwh_f_player_boxscore
WHERE player_id = :id AND points >= 30   -- removes non-qualifying games!
ORDER BY game_date;
-- then row-numbering these makes every game look consecutive
```

**This rule applies to ALL streak types:** scoring streaks, plus-minus streaks, double-double streaks, steal streaks, assist streaks, zero-turnover streaks, three-pointer streaks, any consecutive-game threshold.

**Confirmed failures fixed:** Rows 83, 104, 109, 133. **Confirmed successes:** Rows 66, 142, 149, 179.

---

## RULE 9 — Month / Date-Range Filters

For "this month", "in January", "over the last 3 months" — filter using EXTRACT or DATE_TRUNC on game_date. Never use game_id ranges.

```sql
-- This month
WHERE EXTRACT(MONTH FROM g.game_date) = EXTRACT(MONTH FROM CURRENT_DATE)
  AND EXTRACT(YEAR  FROM g.game_date) = EXTRACT(YEAR  FROM CURRENT_DATE)

-- Specific named month + year
WHERE DATE_TRUNC('month', g.game_date) = DATE '2025-01-01'

-- All games this year (calendar year, not NBA season)
WHERE EXTRACT(YEAR FROM g.game_date) = EXTRACT(YEAR FROM CURRENT_DATE)
```

**Confirmed successes:** Rows 70, 83.

---

## RULE 10 — Threshold Filters: Strict > Not >= for Denominators

When filtering for "perfect" shooting, "minimum N attempts", or any ratio threshold — the denominator must be **strictly greater than zero**. `>= 0` allows zero-attempt games to qualify as "perfect", producing false positives.

```sql
-- CORRECT: at least 1 attempt
WHERE pb.field_goals_attempted > 0
  AND pb.field_goals_made = pb.field_goals_attempted

-- FT% with minimum attempts
WHERE pb.free_throws_attempted >= 10   -- explicit minimum specified by user
  AND pb.free_throws_made::numeric / pb.free_throws_attempted = 1.0

-- WRONG: 0 made / 0 attempted = 1.0 — false positive
WHERE pb.field_goals_attempted >= 0
```

**Confirmed failure fixed:** Row 3. **Confirmed success:** Row 48.

---

## RULE 11 — Foul Outs: fouls_personal >= 6

```sql
SELECT COUNT(DISTINCT pb.game_id) AS games_fouled_out
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g ON pb.game_id = g.game_id
WHERE p.full_name ILIKE '%Kawhi Leonard%'
  AND pb.fouls_personal >= 6
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%');
```

**Confirmed success:** Row 125.

---

## RULE 12 — Player Team Identity: pb.team_id Only, Never Infer From Game Table

When filtering player stats to a specific team, always use `pb.team_id`. Never join the game table to both home/visitor teams and then pull all player stats — this includes opponent players and produces completely wrong results.

```sql
-- CORRECT: pb.team_id identifies exactly which team the player was playing for
WITH target_team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Timberwolves%')
SELECT p.full_name, SUM(pb.points) AS total_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g ON pb.game_id = g.game_id
JOIN target_team t ON pb.team_id = t.team_id    -- pb.team_id only
WHERE g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
  AND g.game_type ILIKE '%Regular Season%'
GROUP BY p.full_name ORDER BY total_points DESC LIMIT 1;

-- WRONG: OR join collects ALL players in Timberwolves games — includes opponents
JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
WHERE t.full_name ILIKE '%Timberwolves%'
-- then stats from all player_ids in those game_ids includes the opposing team's players
```

**Confirmed failures fixed:** Rows 120 (partial), 156. **Confirmed successes:** Rows 117, 132, 204, 205.

---

## RULE 13 — Avoid LIMIT Inside Analytical CTEs

Never apply LIMIT inside CTEs used for intermediate analysis, trends, or streaks. Apply LIMIT only on the final SELECT.

```sql
-- WRONG: silently drops rows mid-analysis
WITH data AS (SELECT ... FROM dwh_f_player_boxscore LIMIT 1000)

-- CORRECT: LIMIT only on final output
SELECT ... FROM analysis_cte ORDER BY season_year LIMIT 10;
```

**Confirmed failure fixed:** Row 129.

---

## RULE 14 — NULL Handling

```sql
-- CORRECT
WHERE school IS NOT NULL

-- WRONG
WHERE school = NULL
```

---

## Anti-Pattern Summary

| Bad Pattern | Root Cause | Fix |
|---|---|---|
| `full_name = 'Jimmy Butler'` | Misses "Jimmy Butler III" | `ILIKE '%Jimmy Butler%'` |
| `full_name = 'Gary Payton'` | Misses "Gary Payton II" | `ILIKE '%Gary Payton%'` |
| `conference = 'Eastern Conference'` | Stored as `'East'` | `ILIKE '%East%'` |
| `game_type = 'Playoffs'` | Case/value mismatch | `ILIKE '%playoff%'` |
| `game_date = CURRENT_DATE - 1` | No game = no result | `ORDER BY game_date DESC LIMIT 1` |
| `ORDER BY game_id DESC` for recency | game_id ≠ date order | `ORDER BY game_date DESC` |
| No season filter on present-tense query | Returns retired players | `g.season_year = (SELECT MAX...)` |
| Season filter on career query | Excludes prior seasons | Remove season_year condition |
| Current season filter for past rookie | Player not found | Use `MIN(season_year)` from player_team_seasons |
| Pre-filter before streak calc | Removes gaps, inflates streaks | Full sequence + window functions |
| `attempts >= 0` for threshold | Zero attempts qualify | `attempts > 0` |
| `LIMIT 1000` inside CTE | Silently drops data | LIMIT on final output only |
| OR join on game teams for player stats | Includes opponents | Use `pb.team_id` |
