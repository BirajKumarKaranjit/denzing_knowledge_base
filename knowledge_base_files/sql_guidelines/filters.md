---
name: filters
description: "Use when the query involves filtering data by player name, team name, game type, season, date range, numeric thresholds, categorical values, streak calculations, or NULL handling. Essential for WHERE clauses involving any entity name lookup, default scoping to current season and regular season, last-game resolution, and all consecutive-game streak logic. Must be consulted for any query mentioning a player or team name, a time period, or a performance threshold."
tags: [filters, WHERE clause, player name, team name, ILIKE, streak, season scoping, last game, date filter, NULL, threshold, game type, regular season, current season, present tense]
priority: critical
---

# SQL Filtering Guidelines

---

## RULE 1 — Entity Name Matching: Always Use ILIKE with Wildcards

Player and team names in the database may include suffixes (Jr., II, III), accents (Jokić, Dončić), or variations. **Never use `=` for any name-based filter. Always use `ILIKE '%name%'`.**

This is non-negotiable. A single `=` match on a name will silently return zero rows if the stored value differs in any way.

```sql
-- CORRECT: catches "Jimmy Butler III", "Gary Payton II", "Nikola Jokić", etc.
WHERE p.full_name ILIKE '%Jimmy Butler%'
WHERE p.full_name ILIKE '%Gary Payton%'
WHERE p.full_name ILIKE '%Nikola Jokic%'    -- accent variation still matches

-- WRONG: misses suffixed or accented names entirely, returns zero rows
WHERE p.full_name = 'Jimmy Butler'
WHERE p.full_name = 'Gary Payton'
WHERE p.full_name = 'Nikola Jokić'
```

Apply this rule identically to team names, game types, positions, conferences, and any other string-valued column.

---

## RULE 2 — Categorical / Enum Fields: Use ILIKE, Never Strict Equality

Stored categorical values (game_type, conference, status, position) vary in casing and phrasing across the database. Always use `ILIKE` with a partial wildcard for these fields.

```sql
-- CORRECT: tolerant categorical match
WHERE g.game_type ILIKE '%Regular Season%'
WHERE g.game_type ILIKE '%playoff%'
WHERE t.conference ILIKE '%East%'
WHERE t.conference ILIKE '%West%'

-- WRONG: brittle — fails if stored value has different casing or phrasing
WHERE g.game_type = 'Playoffs'
WHERE t.conference = 'Eastern Conference'   -- stored as 'East', not 'Eastern Conference'
WHERE t.conference = 'Western Conference'   -- stored as 'West', not 'Western Conference'
```

**Known conference values in the database:** `'East'` and `'West'` — not the full names.

---

## RULE 3 — Default Game Type: Regular Season Unless Specified

When a user does not mention a game type, **always default to regular season**. Never include preseason. Never mix regular season with playoff data unless the user explicitly asks for career totals or all-time records.

```sql
-- Always include unless user explicitly says "career", "all-time", or "playoffs"
AND g.game_type ILIKE '%Regular Season%'
```

---

## RULE 4 — Default Season: Current Season for Present-Tense Queries

When a user uses present tense ("this season", "this year", "currently", "now", "leads", "averages") or asks about a team's current performance, always scope to the latest season in the data. **Never return all-time data for a present-tense question.**

Failing to apply a season filter causes retired players to appear as current leaders, which is a critical UX failure.

```sql
-- CORRECT: scope to current season
AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')

-- WRONG: no season filter returns all-time data including retired players
-- This makes LeBron James appear as a current scorer when asking about active leaders
```

**Trigger words that REQUIRE a current season filter:**
- "this season", "this year", "currently", "now", "leads", "right now", "2024-25", "averaging"
- "who is the best", "who leads", "top scorer", "most points this season"

**Trigger words that allow all-time / career data:**
- "career", "all-time", "ever", "history", "historically", "in his career"

---

## RULE 5 — "Last Game" Resolution: Use ORDER BY game_date DESC, Never CURRENT_DATE

When the user asks about "last night", "most recent game", "last game", always resolve dynamically from the data. Never hardcode `CURRENT_DATE - 1` or any fixed date offset.

```sql
-- CORRECT: finds the actual latest game in the data
WHERE pb.game_id = (
    SELECT pb2.game_id
    FROM dwh_f_player_boxscore pb2
    JOIN dwh_d_games g2 ON pb2.game_id = g2.game_id
    WHERE pb2.player_id = pb.player_id
    ORDER BY g2.game_date DESC
    LIMIT 1
)

-- CORRECT: for team last game
WHERE g.game_date = (
    SELECT MAX(g2.game_date)
    FROM dwh_d_games g2
    JOIN dwh_d_teams t2 ON g2.home_team_id = t2.team_id OR g2.visitor_team_id = t2.team_id
    WHERE t2.full_name ILIKE '%Heat%'
)

-- WRONG: fails if no game was played yesterday
WHERE g.game_date = CURRENT_DATE - 1
```

---

## RULE 6 — Threshold Filters: Use Strict Greater-Than for Denominators

When filtering for "perfect" ratios (100% shooting, 100% free throws) or any minimum-attempt condition, the denominator must be **strictly greater than zero**. Using `>= 0` allows zero-attempt games to appear as "perfect", producing false positives.

```sql
-- CORRECT: requires at least 1 attempt
WHERE pb.field_goals_attempted > 0
  AND pb.field_goals_made = pb.field_goals_attempted

-- WRONG: 0 made = 0 attempted qualifies as "perfect" — false positive
WHERE pb.field_goals_attempted >= 0
  AND pb.field_goals_made = pb.field_goals_attempted
```

---

## RULE 7 — Streak Calculation: Never Pre-Filter, Always Use Full Game Sequence

**This is the most common streak bug.** Pre-filtering rows that meet a condition (e.g., games with 30+ points, or games with positive plus-minus) removes the gaps between qualifying games, making the row numbers contiguous and inflating streak lengths to incorrect values.

The correct pattern: keep all games in the sequence for the player, flag each game as qualifying or not, then use the gaps-and-islands technique to find streaks.

```sql
-- CORRECT: keep ALL games, flag qualifying ones, compute streaks on full sequence
WITH all_games AS (
    SELECT
        pb.player_id,
        g.game_date,
        pb.points,
        ROW_NUMBER() OVER (PARTITION BY pb.player_id ORDER BY g.game_date) AS rn,
        CASE WHEN pb.points >= 10 THEN 1 ELSE 0 END AS qualifies
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g ON pb.game_id = g.game_id
    WHERE p.full_name ILIKE '%Player Name%'
      AND g.game_type ILIKE '%Regular Season%'
),
streaks AS (
    SELECT *,
        rn - ROW_NUMBER() OVER (
            PARTITION BY player_id, qualifies ORDER BY game_date
        ) AS grp
    FROM all_games
)
SELECT
    MIN(game_date) AS streak_start,
    MAX(game_date) AS streak_end,
    COUNT(*) AS streak_length
FROM streaks
WHERE qualifies = 1
GROUP BY player_id, grp
ORDER BY streak_length DESC
LIMIT 1;

-- WRONG: filter first, then number — removes gaps, inflates streaks
WITH qualifying_games AS (
    SELECT * FROM dwh_f_player_boxscore
    WHERE player_id = :id AND points >= 10   -- removes non-qualifying gaps!
)
SELECT ROW_NUMBER() OVER (ORDER BY game_date) ...
```

**This rule applies to all streak types:** consecutive scoring games, positive plus-minus streaks, consecutive double-doubles, consecutive steal games, back-to-back threshold games, and any other consecutive-game pattern.

---

## RULE 8 — Season Scoping: Match the Question's Time Context

Do not default blindly to the current season for all queries. Match the season scope to what the user is asking.

```sql
-- Current season
AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')

-- Previous / "last" season
AND g.season_year = (
    SELECT MAX(season_year) FROM dwh_d_games
    WHERE season_year < (SELECT MAX(season_year) FROM dwh_d_games)
      AND game_type ILIKE '%Regular Season%'
)

-- Rookie season (determine dynamically, never hardcode)
AND g.season_year = (
    SELECT MIN(season_year) FROM dwh_f_player_team_seasons WHERE player_id = :player_id
)

-- Career / all-time: no season filter at all
```

**Bronny James pattern:** When a user asks about a rookie who played in a prior season, do not apply the current season filter. Determine the rookie season dynamically from `dwh_f_player_team_seasons` using `MIN(season_year)`.

---

## RULE 9 — Avoid LIMIT in Analytical CTEs

Never apply `LIMIT` inside CTEs used for intermediate analysis (trends, streaks, aggregations). Arbitrary limits silently truncate data and corrupt results. Apply `LIMIT` only on the final output row.

```sql
-- WRONG: LIMIT 1000 silently drops rows mid-analysis
WITH data AS (
    SELECT ... FROM dwh_f_player_boxscore LIMIT 1000
)

-- CORRECT: LIMIT only on final output
SELECT ... FROM analysis_cte ORDER BY season_year LIMIT 10;
```

---

## RULE 10 — NULL Handling

Use `IS NULL` / `IS NOT NULL`. Never compare NULL with `=`.

```sql
-- CORRECT
WHERE school IS NOT NULL

-- WRONG
WHERE school = NULL
```

---

## RULE 11 — Player Team Identity: Use pb.team_id, Never Game Table

When identifying which team a player belongs to in a game, always use `pb.team_id` from the boxscore. **Never infer player team from `g.home_team_id` or `g.visitor_team_id`** — those identify both teams in the game, not the player's team. Using the game table to infer team membership includes opponent players and produces wildly incorrect results.

```sql
-- CORRECT: player's team is pb.team_id
WHERE pb.team_id = (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Timberwolves%')

-- WRONG: joins game to both teams, includes opponent players in the result
JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
WHERE t.full_name ILIKE '%Timberwolves%'
-- then scores ALL players in those games, not just Timberwolves players
```

See also Joins guidelines for the full canonical pattern.

---

## Priority Quick-Reference

| Default Rule | SQL Pattern |
|---|---|
| Game type when unspecified | `AND g.game_type ILIKE '%Regular Season%'` |
| Season when present-tense | `AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')` |
| Last game resolution | `ORDER BY g.game_date DESC LIMIT 1` — never `CURRENT_DATE - 1` |
| Player name match | `ILIKE '%name%'` — never `=` |
| Team name match | `ILIKE '%name%'` — never `=` |
| Categorical enum match | `ILIKE '%value%'` — never `=` |
| Conference values | `'East'` and `'West'` — not full names |
| Denominator safety | `> 0` not `>= 0` |
| Streak logic | Full unfiltered sequence + window function — never pre-filter |
| Limit placement | Final output only — never inside analytical CTEs |

---

## Anti-Pattern Summary

| Bad Pattern | Root Cause | Correct Fix |
|---|---|---|
| `full_name = 'Jimmy Butler'` | Misses suffixed names | `full_name ILIKE '%Jimmy Butler%'` |
| `game_type = 'Playoffs'` | Case/value mismatch | `game_type ILIKE '%playoff%'` |
| `conference = 'Eastern Conference'` | Wrong stored value | `conference ILIKE '%East%'` |
| `game_date = CURRENT_DATE - 1` | No game = no result | `ORDER BY game_date DESC LIMIT 1` |
| `attempts >= 0` | Zero attempts = false positive | `attempts > 0` |
| Pre-filter rows before streak calc | Removes gaps, inflates streaks | Keep all rows, use window functions |
| `LIMIT 1000` inside CTE | Silently drops analytical data | Move LIMIT to final output |
| No season filter on present-tense query | Returns retired players | Add `g.season_year = (SELECT MAX(...))` |
| Inferring player team from game table | Includes opponent players | Use `pb.team_id` directly |
