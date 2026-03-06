---
name: aggregations
description: "Use when the query involves totals, averages, counts, per-game rates, per-36-minute stats, consistency metrics, plus-minus aggregations, streak lengths, triple-doubles, double-doubles, SUM vs AVG decisions, season grouping, or any GROUP BY pattern. Also covers safe division, sample-size-weighted consistency, correct season-year grouping, and output labeling requirements."
tags: [aggregations, SUM, AVG, GROUP BY, per-game, per-36, consistency, triple-double, double-double, plus-minus, streak, season grouping, safe division, window functions, output labels]
priority: critical
---

# SQL Aggregation Guidelines

---

## RULE 1 — SUM vs AVG: Match the Aggregation to the User's Intent

Always map the user's phrasing to the correct aggregation. Using AVG when SUM is requested (or vice versa) produces wrong answers that are not caught by SQL execution — they silently return an incorrect number.

| User phrasing | Aggregation to use |
|---|---|
| "total", "how many overall", "career points", "all-time" | `SUM` |
| "per game", "average", "on average", "averaging" | `AVG` |
| "per 36 minutes", "per 36" | Weighted SUM formula (see Rule 3) |
| "who leads in plus-minus" | `SUM(plus_minus_points)` — total impact, not per-game rate |

```sql
-- User asks "who are the best players by plus-minus?"
-- CORRECT: total plus-minus (SUM)
SELECT p.full_name, SUM(pb.plus_minus_points) AS total_plus_minus
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
GROUP BY p.full_name
ORDER BY total_plus_minus DESC;

-- WRONG: AVG gives per-game rate, not total impact
SELECT p.full_name, AVG(pb.plus_minus_points) AS avg_plus_minus  -- wrong for "best" ranking
```

---

## RULE 2 — Season Grouping: Always Use g.season_year, Never DATE_TRUNC

When grouping or filtering by season, use the `g.season_year` column from `dwh_d_games`. Never use `DATE_TRUNC('year', game_date)` — this groups by calendar year (Jan–Dec), which splits NBA seasons (Oct–Jun) across two calendar years, producing corrupted double-grouping of data.

```sql
-- CORRECT: group by NBA season
SELECT g.season_year, AVG(pb.plus_minus_points) AS avg_plus_minus
FROM dwh_f_player_boxscore pb
JOIN dwh_d_games g ON pb.game_id = g.game_id
WHERE p.full_name ILIKE '%Giannis%'
  AND g.game_type ILIKE '%Regular Season%'
GROUP BY g.season_year
ORDER BY g.season_year DESC;

-- WRONG: calendar-year grouping splits NBA seasons — Oct-Dec goes to one year, Jan-Jun to another
SELECT DATE_TRUNC('year', g.game_date) AS year, AVG(pb.plus_minus_points)
FROM ...
GROUP BY DATE_TRUNC('year', g.game_date);   -- season 2024-25 becomes 2024 AND 2025!
```

---

## RULE 3 — Per-36 Stats: Weight by Minutes Played, Never Simple Average

Per-36 minute stats must be calculated by summing the raw stat, dividing by total minutes, and scaling to 36. A simple average of game-level rates gives equal weight to a 3-minute garbage-time appearance as a 40-minute full game, producing heavily distorted results.

```sql
-- CORRECT: weighted by total minutes
SELECT
    p.full_name,
    SUM(pb.points)   / NULLIF(SUM(pb.minutes_played), 0) * 36 AS pts_per_36,
    SUM(pb.assists)  / NULLIF(SUM(pb.minutes_played), 0) * 36 AS ast_per_36,
    SUM(pb.rebounds_offensive + pb.rebounds_defensive)
                     / NULLIF(SUM(pb.minutes_played), 0) * 36 AS reb_per_36
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
WHERE p.full_name ILIKE '%Stephen Curry%'
  AND pb.minutes_played > 0
GROUP BY p.full_name;

-- WRONG: treats 3-minute game the same as 38-minute game
SELECT AVG(pb.points / NULLIF(pb.minutes_played, 0) * 36) FROM ...;
```

---

## RULE 4 — Per-Game Stats: Divide by COUNT(DISTINCT game_id)

When computing per-game averages from raw totals, always divide by `COUNT(DISTINCT pb.game_id)`. Using `AVG()` directly is acceptable when each row is already one game per player. Using `SUM() / COUNT(game_id)` without `DISTINCT` can double-count if joins produce multiple rows.

```sql
-- CORRECT
SELECT
    p.full_name,
    SUM(pb.points) / NULLIF(COUNT(DISTINCT pb.game_id), 0) AS points_per_game
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
GROUP BY p.full_name;
```

---

## RULE 5 — Consistency Metrics: Always Weight by Sample Size

Low standard deviation alone does not indicate consistency. A player with 3 games will have trivially low variance and will dominate any ranking by `STDDEV ASC`. Always incorporate sample size using a coefficient of variation multiplied by a sample weight, and derive the minimum game threshold dynamically from the data.

```sql
-- CORRECT: coefficient of variation weighted by sample size
WITH stats AS (
    SELECT
        pb.player_id,
        COUNT(*)        AS games,
        AVG(pb.points)  AS avg_pts,
        STDDEV(pb.points) AS std_pts,
        AVG(pb.points) / NULLIF(STDDEV(pb.points), 0) AS consistency_ratio
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_games g ON pb.game_id = g.game_id
    WHERE g.game_type ILIKE '%Regular Season%'
    GROUP BY pb.player_id
),
min_sample AS (
    SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY games) AS min_games FROM stats
)
SELECT
    s.player_id,
    s.avg_pts,
    s.std_pts,
    s.games,
    s.consistency_ratio * LEAST(s.games::numeric / ms.min_games, 1) AS weighted_consistency
FROM stats s, min_sample ms
WHERE s.games >= ms.min_games
ORDER BY weighted_consistency DESC;

-- WRONG: player with 3 games wins due to trivially low variance
SELECT player_id, STDDEV(points) FROM ... ORDER BY STDDEV(points) ASC LIMIT 1;
```

**Rule:** Derive minimum meaningful sample size dynamically. Never hardcode it (e.g., `>= 20`).

---

## RULE 6 — Triple-Double and Double-Double Detection: Check All 5 Categories

A triple-double requires 10+ in any 3 of 5 categories: points, rebounds (offensive + defensive), assists, steals, blocks. Do not limit the check to only points/rebounds/assists — this misses valid triple-doubles involving steals or blocks.

```sql
-- CORRECT: all 5 categories checked
(CASE WHEN pb.points >= 10 THEN 1 ELSE 0 END
 + CASE WHEN (pb.rebounds_offensive + pb.rebounds_defensive) >= 10 THEN 1 ELSE 0 END
 + CASE WHEN pb.assists >= 10 THEN 1 ELSE 0 END
 + CASE WHEN pb.steals >= 10 THEN 1 ELSE 0 END
 + CASE WHEN pb.blocks >= 10 THEN 1 ELSE 0 END) >= 3

-- WRONG: misses steals/blocks combinations
WHERE pb.points >= 10 AND (pb.rebounds_offensive + pb.rebounds_defensive) >= 10 AND pb.assists >= 10
```

A **double-double** uses the same pattern with threshold >= 2.

A **quadruple-double** uses threshold >= 4.

---

## RULE 7 — Averages by Group (e.g., Minutes by Age): Always AVG, Never SUM

When grouping by a demographic dimension like age and computing a rate metric, use `AVG`. Using `SUM` computes a total across all players of that age rather than the average rate.

```sql
-- CORRECT: average minutes per game by age
SELECT
    EXTRACT(YEAR FROM AGE(g.game_date, p.birthdate))::int AS age,
    AVG(pb.minutes_played) AS avg_minutes
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
GROUP BY age
ORDER BY age;

-- WRONG: SUM gives total accumulated minutes for all players at that age
SELECT age, SUM(minutes_played) FROM ... GROUP BY age;
```

---

## RULE 8 — Player Efficiency Rating (PER)

PER requires league averages, pace adjustment, and normalization. These values are not available in the database as stored columns.

- If a `per` column exists → use it directly.
- If no `per` column exists → do not attempt to compute PER. Label any simplified formula clearly as "Simple Efficiency Rating", not PER.

Never reference a `per` column without confirming it exists in the schema first.

---

## RULE 9 — Safe Division: Always Use NULLIF

All division operations must wrap the denominator in `NULLIF(..., 0)` to prevent division-by-zero errors.

```sql
-- CORRECT
field_goals_made::numeric / NULLIF(field_goals_attempted, 0) * 100 AS fg_pct

-- WRONG
field_goals_made / field_goals_attempted * 100   -- crashes on 0 attempts
```

---

## RULE 10 — Output Labeling: Always Include Player Name in Output

Any aggregation result exposed to the user must include `p.full_name AS player_name` when the query involves players. A result showing only numeric IDs (`player_id`) with statistics has zero usability. Join `dwh_d_players` even if the question does not explicitly ask for names — users need names to understand results.

```sql
-- CORRECT: user can read the result
SELECT p.full_name AS player_name, SUM(pb.points) AS career_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
GROUP BY p.full_name;

-- WRONG: result is unreadable
SELECT pb.player_id, SUM(pb.points) AS career_points
FROM dwh_f_player_boxscore pb
GROUP BY pb.player_id;
```

---

## RULE 11 — GROUP BY Completeness

All non-aggregated columns in SELECT must appear in GROUP BY. PostgreSQL enforces this strictly.

If a column is a guaranteed single value (e.g., from a one-row CTE), wrap it in `MAX()` or `MIN()` to satisfy PostgreSQL rather than adding it to GROUP BY unnecessarily.

---

## RULE 12 — Type Consistency in Arithmetic

When performing arithmetic between season_year and integer offsets, ensure type consistency. `season_year` is stored as text in this schema — cast it before arithmetic.

```sql
-- CORRECT: cast season_year to numeric before arithmetic
WHERE g.season_year::integer >= (SELECT MAX(season_year)::integer FROM dwh_d_games) - 1

-- WRONG: text - integer crashes
WHERE g.season_year >= (SELECT MAX(season_year) FROM dwh_d_games) - 1
```

---

## Quick-Reference Aggregation Decision Table

| User phrasing | Formula |
|---|---|
| Total / career / all-time | `SUM(stat)` |
| Average / per game | `AVG(stat)` or `SUM(stat) / NULLIF(COUNT(DISTINCT game_id), 0)` |
| Per 36 minutes | `SUM(stat) / NULLIF(SUM(minutes_played), 0) * 36` |
| Win percentage | `SUM(wins)::numeric / NULLIF(SUM(wins + losses), 0)` |
| Field goal % | `SUM(fg_made)::numeric / NULLIF(SUM(fg_attempted), 0) * 100` |
| True shooting % | `SUM(pts) / NULLIF(2 * (SUM(fga) + 0.44 * SUM(fta)), 0) * 100` |
| Net rating | `AVG(offensive_rating) - AVG(defensive_rating)` |
| eFG% | `SUM(fg_made + 0.5 * three_made)::numeric / NULLIF(SUM(fg_attempted), 0)` |
| Consistency | Coefficient of variation × sample weight (see Rule 5) |

---

## RULE 13 — Career / All-Time Queries: No Season Filter

When the user asks "in his career", "all-time", "ever", "how many total in his career", "career high", do NOT apply a season filter. Include all game types (regular season + playoffs) unless the user says "regular season only".

```sql
-- Career total points (no season, no game_type filter)
SELECT p.full_name, SUM(pb.points) AS career_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
WHERE p.full_name ILIKE '%LeBron James%'
GROUP BY p.full_name;

-- Career high in a single game stat
SELECT p.full_name, MAX(pb.assists) AS career_high_assists, g.game_date
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g ON pb.game_id = g.game_id
WHERE p.full_name ILIKE '%LeBron James%'
ORDER BY pb.assists DESC
LIMIT 1;
```

---

## RULE 14 — Team Championships: Use dwh_f_team_championships

Championship queries use `dwh_f_team_championships` joined with `dwh_d_teams`. Use `MAX(yearawarded)` for most recent champion. Use `COUNT(*)` for total championships.

```sql
-- Most recent champion
SELECT t.full_name AS champion, tc.yearawarded, tc.oppositeteam AS runner_up
FROM dwh_f_team_championships tc
JOIN dwh_d_teams t ON tc.team_id = t.team_id
WHERE tc.yearawarded = (SELECT MAX(yearawarded) FROM dwh_f_team_championships)
LIMIT 1;

-- Total championships for a team
SELECT COUNT(*) AS championships_won
FROM dwh_f_team_championships tc
JOIN dwh_d_teams t ON tc.team_id = t.team_id
WHERE t.full_name ILIKE '%Los Angeles Lakers%';

-- Playoff teams in a specific season (teams that appear in playoff games)
SELECT DISTINCT t.full_name AS team_name, t.abbreviation, t.city
FROM dwh_d_games g
JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
WHERE g.season_year = '2022'
  AND g.game_type ILIKE '%playoff%'
ORDER BY t.full_name;
```

---

## RULE 15 — Shooting Efficiency Formulas: Compute from Base Columns

These metrics must be computed from base columns — never assume a pre-computed column exists.

```sql
-- True Shooting % (TS%)
SUM(pb.points)::numeric / NULLIF(2 * (SUM(pb.field_goals_attempted) + 0.44 * SUM(pb.free_throws_attempted)), 0) * 100 AS true_shooting_pct

-- Effective Field Goal % (eFG%)
SUM(pb.field_goals_made + 0.5 * pb.three_pointers_made)::numeric / NULLIF(SUM(pb.field_goals_attempted), 0) AS efg_pct

-- Field Goal %
SUM(pb.field_goals_made)::numeric / NULLIF(SUM(pb.field_goals_attempted), 0) * 100 AS fg_pct

-- Three Point %
SUM(pb.three_pointers_made)::numeric / NULLIF(SUM(pb.three_pointers_attempted), 0) * 100 AS three_pt_pct

-- Free Throw %
SUM(pb.free_throws_made)::numeric / NULLIF(SUM(pb.free_throws_attempted), 0) * 100 AS ft_pct

-- Assist-to-Turnover Ratio
SUM(pb.assists)::numeric / NULLIF(SUM(pb.turnovers), 0) AS ast_to_tov_ratio

-- Net Rating (team boxscore)
AVG(tb.estimated_offensive_rating) - AVG(tb.estimated_defensive_rating) AS net_rating

-- Win Percentage
SUM(wins)::numeric / NULLIF(SUM(wins + losses), 0) * 100 AS win_pct
```

---

## RULE 16 — Team Win/Loss Record with Win Percentage

The canonical pattern for team win/loss records. Used for: season record, home record, away record, record against conference, record in last N games.

```sql
-- Full season record
WITH team_games AS (
    SELECT
        SUM(CASE WHEN g.home_team_id = t.team_id AND g.home_score > g.visitor_score THEN 1
                 WHEN g.visitor_team_id = t.team_id AND g.visitor_score > g.home_score THEN 1
                 ELSE 0 END) AS wins,
        SUM(CASE WHEN g.home_team_id = t.team_id AND g.home_score < g.visitor_score THEN 1
                 WHEN g.visitor_team_id = t.team_id AND g.visitor_score < g.home_score THEN 1
                 ELSE 0 END) AS losses
    FROM dwh_d_games g
    JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
    WHERE t.full_name ILIKE '%Golden State Warriors%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
      AND g.game_type ILIKE '%Regular Season%'
)
SELECT wins, losses,
    ROUND(wins::numeric / NULLIF(wins + losses, 0) * 100, 1) AS win_pct
FROM team_games;

-- Away record only (visitor wins)
SELECT
    t.full_name AS team_name,
    SUM(CASE WHEN g.visitor_score > g.home_score THEN 1 ELSE 0 END) AS away_wins,
    SUM(CASE WHEN g.visitor_score < g.home_score THEN 1 ELSE 0 END) AS away_losses
FROM dwh_d_games g
JOIN dwh_d_teams t ON g.visitor_team_id = t.team_id
WHERE t.full_name ILIKE '%Los Angeles Lakers%'
  AND g.game_type ILIKE '%Regular Season%'
GROUP BY t.full_name;

-- Home record only
SELECT
    t.full_name AS team_name,
    SUM(CASE WHEN g.home_score > g.visitor_score THEN 1 ELSE 0 END) AS home_wins,
    SUM(CASE WHEN g.home_score < g.visitor_score THEN 1 ELSE 0 END) AS home_losses
FROM dwh_d_games g
JOIN dwh_d_teams t ON g.home_team_id = t.team_id
WHERE t.full_name ILIKE '%Phoenix Suns%'
  AND g.game_type ILIKE '%Regular Season%'
GROUP BY t.full_name;
```

---

## RULE 17 — Teams a Player Has Played For: Use DISTINCT on Season Table

To find all teams a player has played for, use `dwh_f_player_team_seasons` joined to `dwh_d_teams` with `DISTINCT`. Do not try to derive this from boxscore team_id alone — the season table is the authoritative source.

```sql
SELECT DISTINCT t.full_name AS team_name
FROM dwh_f_player_team_seasons pts
JOIN dwh_d_players p ON pts.player_id = p.player_id
JOIN dwh_d_teams t   ON pts.team_id   = t.team_id
WHERE p.full_name ILIKE '%Donovan Mitchell%'
ORDER BY t.full_name;
```

---

## RULE 18 — Player Lookup with LEFT JOIN for Sparse Data

When a player may have limited data (new player, inactive player, G-League player), use LEFT JOIN to teams so the player record is still returned even if team data is missing.

```sql
SELECT p.full_name, p.position, p.player_id,
    t.full_name AS team_name
FROM dwh_d_players p
LEFT JOIN dwh_d_teams t ON p.team_id = t.team_id
WHERE p.full_name ILIKE '%Cooper Flagg%';
```

---

## Anti-Pattern Summary

| Bad Pattern | Consequence | Fix |
|---|---|---|
| `AVG(plus_minus_points)` for ranking | Per-game rate, not total impact | `SUM(plus_minus_points)` |
| `DATE_TRUNC('year', game_date)` for season | Splits NBA season across two calendar years | Use `g.season_year` |
| `AVG(stat / minutes * 36)` for per-36 | Garbage-time games inflate result | `SUM(stat) / SUM(minutes) * 36` |
| `STDDEV ASC LIMIT 1` for consistency | Player with 3 games wins | Apply sample-weight multiplier |
| Check only pts/reb/ast for triple-double | Misses steals/blocks combos | Check all 5 categories |
| `SUM(minutes)` grouped by age | Total minutes, not average rate | Use `AVG(minutes)` |
| `SELECT player_id` without joining name | Unreadable output | Always join `dwh_d_players` for name |
| `season_year - 1` without cast | Type error on text field | `season_year::integer - 1` |
| Division without NULLIF | Crash on zero denominator | `/ NULLIF(denominator, 0)` |
| Season filter on career query | Excludes prior seasons | No season filter for "career", "ever", "all-time" |
| Assume net_rating column exists | Column does not exist | Compute as `offensive_rating - defensive_rating` |
| JOIN on game teams for player career teams | Misses traded seasons | Use `dwh_f_player_team_seasons` DISTINCT |
