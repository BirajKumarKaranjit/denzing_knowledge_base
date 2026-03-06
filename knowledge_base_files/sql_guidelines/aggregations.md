---
name: aggregations
description: "Use when the query involves calculating totals, averages, per-game rates, per-36-minute stats, shooting percentages (FG%, eFG%, TS%, 3P%, FT%), assist-to-turnover ratio, win percentage, consistency metrics, season grouping, triple-double detection, double-double detection, scoring streaks, plus-minus ranking, career stats, season stats, or any aggregation across players, teams, or time periods. Contains formulas for all standard NBA efficiency metrics and explicit SUM vs AVG decision rules."
tags: [aggregations, SUM, AVG, GROUP BY, career totals, per-game, per-36, FG%, eFG%, TS%, win percentage, triple-double, double-double, consistency, plus-minus, season grouping, season_year, shooting formulas, A/TO ratio, NULLIF, safe division, COUNT DISTINCT]
priority: critical
---

# SQL Aggregation Guidelines

---

## RULE 1 — SUM vs AVG: Always Match the User's Intent

| User Phrase | Use |
|---|---|
| "total", "career", "how many overall", "scored in his career" | `SUM` |
| "per game", "average", "averaging", "on average" | `AVG` |
| "per 36 minutes" | Weighted calculation — see Rule 4 |
| "ranking by plus-minus", "best plus-minus players" | `SUM` — total impact, not per-game rate |
| "minutes by age", "average minutes per age" | `AVG` — rate by group, not total |

```sql
-- Total career points (SUM)
SELECT p.full_name, SUM(pb.points) AS career_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
WHERE p.full_name ILIKE '%LeBron James%'
GROUP BY p.full_name;

-- Per-game average this season (AVG)
SELECT p.full_name, AVG(pb.points) AS ppg
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
WHERE p.full_name ILIKE '%Stephen Curry%'
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name;

-- Plus-minus ranking — use SUM not AVG
SELECT p.full_name, SUM(pb.plus_minus_points) AS total_plus_minus
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
WHERE g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name
ORDER BY total_plus_minus DESC LIMIT 10;

-- WRONG for ranking: AVG gives per-game rate, not total impact
SELECT player_id, AVG(plus_minus_points) FROM ... ORDER BY AVG DESC;
```

**Confirmed failure fixed:** Row 155. **Confirmed successes:** Rows 13, 36, 50, 53, 87, 167, 172.

---

## RULE 2 — Season Grouping: Always g.season_year, Never DATE_TRUNC('year')

NBA seasons span two calendar years (Oct–Jun). `DATE_TRUNC('year')` splits a season across two groups (e.g., games from Oct–Dec 2024 and games from Jan–Jun 2025 count separately). Always use `g.season_year` for season-level grouping.

```sql
-- CORRECT: season grouping
SELECT g.season_year, AVG(pb.points) AS ppg
FROM dwh_f_player_boxscore pb
JOIN dwh_d_games g ON pb.game_id = g.game_id
JOIN dwh_d_players p ON pb.player_id = p.player_id
WHERE p.full_name ILIKE '%LeBron James%'
  AND g.game_type ILIKE '%Regular Season%'
GROUP BY g.season_year
ORDER BY g.season_year;

-- WRONG: splits 2024-25 season into two calendar years
GROUP BY DATE_TRUNC('year', g.game_date)
```

**Confirmed successes:** Rows 184, 186, 187.

---

## RULE 3 — Per-Game Rate: COUNT(DISTINCT game_id) as Denominator

When computing per-game averages from raw totals, divide by `COUNT(DISTINCT pb.game_id)`. Do not use `COUNT(game_id)` without DISTINCT if joins could produce multiple rows per game.

```sql
SUM(pb.points) / NULLIF(COUNT(DISTINCT pb.game_id), 0) AS points_per_game
```

---

## RULE 4 — Per-36 Minutes: Weighted by Total Minutes, Not Simple Average

Per-36 stats require weighting by actual minutes played. A simple `AVG(stat / minutes * 36)` gives equal weight to a 2-minute garbage-time game and a 40-minute game, producing meaningless results.

```sql
-- CORRECT: total stats divided by total minutes, scaled to 36
SELECT p.full_name,
    SUM(pb.points)  / NULLIF(SUM(pb.minutes_played), 0) * 36 AS pts_per_36,
    SUM(pb.assists) / NULLIF(SUM(pb.minutes_played), 0) * 36 AS ast_per_36,
    SUM(pb.rebounds_offensive + pb.rebounds_defensive)
                    / NULLIF(SUM(pb.minutes_played), 0) * 36 AS reb_per_36
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
WHERE p.full_name ILIKE '%Stephen Curry%'
  AND pb.minutes_played > 0
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name;

-- WRONG: treats garbage-time equally
SELECT AVG(points / NULLIF(minutes_played, 0) * 36) FROM ...;
```

**Confirmed failure fixed:** Row 102.

---

## RULE 5 — Shooting Efficiency Formulas

Never assume these metrics exist as pre-computed columns. Always compute from base columns. Use `NULLIF` on all denominators to prevent division by zero.

```sql
-- Field Goal % (FG%)
SUM(pb.field_goals_made)::numeric / NULLIF(SUM(pb.field_goals_attempted), 0) AS fg_pct

-- Three-Point % (3P%)
SUM(pb.three_pointers_made)::numeric / NULLIF(SUM(pb.three_pointers_attempted), 0) AS three_pt_pct

-- Free Throw % (FT%)
SUM(pb.free_throws_made)::numeric / NULLIF(SUM(pb.free_throws_attempted), 0) AS ft_pct

-- Effective Field Goal % (eFG%)
SUM(pb.field_goals_made + 0.5 * pb.three_pointers_made)::numeric
    / NULLIF(SUM(pb.field_goals_attempted), 0) AS efg_pct

-- True Shooting % (TS%)
SUM(pb.points)::numeric
    / NULLIF(2 * (SUM(pb.field_goals_attempted) + 0.44 * SUM(pb.free_throws_attempted)), 0) AS ts_pct

-- Assist-to-Turnover Ratio (A/TO)
SUM(pb.assists)::numeric / NULLIF(SUM(pb.turnovers), 0) AS ast_to_tov_ratio
```

**Confirmed successes:** Rows 53, 71, 78, 96, 103, 116, 138, 139, 196.

---

## RULE 6 — Win Percentage: Compute From CASE WHEN Wins / Total

```sql
ROUND(
    SUM(CASE WHEN (g.home_team_id = t.team_id AND g.home_score > g.visitor_score) OR
                  (g.visitor_team_id = t.team_id AND g.visitor_score > g.home_score)
             THEN 1 ELSE 0 END)::numeric
    / NULLIF(COUNT(*), 0) * 100, 1
) AS win_pct
```

**Confirmed successes:** Rows 86, 98, 165, 178, 187.

---

## RULE 7 — Triple-Double Detection: Check All 5 Categories

A triple-double is 10+ in any 3 of the 5 major categories: points, total rebounds, assists, steals, blocks. Do not limit the check to pts/reb/ast only — this misses steals and blocks combinations.

```sql
-- CORRECT: checks all 5 categories
SELECT COUNT(*) AS triple_doubles
FROM (
    SELECT pb.game_id,
        (CASE WHEN pb.points >= 10 THEN 1 ELSE 0 END +
         CASE WHEN (pb.rebounds_offensive + pb.rebounds_defensive) >= 10 THEN 1 ELSE 0 END +
         CASE WHEN pb.assists >= 10 THEN 1 ELSE 0 END +
         CASE WHEN pb.steals  >= 10 THEN 1 ELSE 0 END +
         CASE WHEN pb.blocks  >= 10 THEN 1 ELSE 0 END) AS double_digit_count
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    WHERE p.full_name ILIKE '%Stephen Curry%'
) sub
WHERE double_digit_count >= 3;

-- Most recent triple-double: add ORDER BY game_date DESC LIMIT 1 for latest
SELECT g.game_date, pb.points,
    pb.rebounds_offensive + pb.rebounds_defensive AS total_reb,
    pb.assists, pb.steals, pb.blocks
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
WHERE p.full_name ILIKE '%Joel Embiid%'
  AND (CASE WHEN pb.points >= 10 THEN 1 ELSE 0 END +
       CASE WHEN (pb.rebounds_offensive + pb.rebounds_defensive) >= 10 THEN 1 ELSE 0 END +
       CASE WHEN pb.assists >= 10 THEN 1 ELSE 0 END +
       CASE WHEN pb.steals  >= 10 THEN 1 ELSE 0 END +
       CASE WHEN pb.blocks  >= 10 THEN 1 ELSE 0 END) >= 3
ORDER BY g.game_date DESC LIMIT 1;

-- WRONG: only checks pts/reb/ast
WHERE pb.points >= 10 AND (rebounds_offensive + rebounds_defensive) >= 10 AND pb.assists >= 10
```

**Confirmed failure fixed:** Row 105. **Confirmed successes:** Rows 23, 66, 107, 111, 190.

---

## RULE 8 — Double-Double Detection

A double-double is 10+ in any 2 of the 5 categories. Same pattern as triple-double.

```sql
SELECT COUNT(*) AS double_doubles
FROM (
    SELECT pb.game_id,
        (CASE WHEN pb.points >= 10 THEN 1 ELSE 0 END +
         CASE WHEN (pb.rebounds_offensive + pb.rebounds_defensive) >= 10 THEN 1 ELSE 0 END +
         CASE WHEN pb.assists >= 10 THEN 1 ELSE 0 END +
         CASE WHEN pb.steals  >= 10 THEN 1 ELSE 0 END +
         CASE WHEN pb.blocks  >= 10 THEN 1 ELSE 0 END) AS double_digit_count
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Gary Payton%'
      AND g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
) sub WHERE double_digit_count >= 2;
```

**Confirmed successes:** Rows 66, 107.

---

## RULE 9 — Wins by Margin: home_score - visitor_score >= N

```sql
-- Wins by 10+ points this season
WITH team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Golden State Warriors%')
SELECT COUNT(*) AS blowout_wins
FROM dwh_d_games g, team t
WHERE ((g.home_team_id = t.team_id AND g.home_score - g.visitor_score >= 10) OR
       (g.visitor_team_id = t.team_id AND g.visitor_score - g.home_score >= 10))
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%');
```

**Confirmed success:** Row 86.

---

## RULE 10 — Overtime Record

```sql
WITH team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Miami Heat%')
SELECT
    SUM(CASE WHEN (g.home_team_id = t.team_id AND g.home_score > g.visitor_score) OR
                  (g.visitor_team_id = t.team_id AND g.visitor_score > g.home_score)
             THEN 1 ELSE 0 END) AS ot_wins,
    SUM(CASE WHEN (g.home_team_id = t.team_id AND g.home_score < g.visitor_score) OR
                  (g.visitor_team_id = t.team_id AND g.visitor_score < g.home_score)
             THEN 1 ELSE 0 END) AS ot_losses
FROM dwh_d_games g, team t
WHERE (g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id)
  AND g.game_type ILIKE '%Regular Season%'
  AND g.home_score + g.visitor_score > 0   -- or use OT period flag if column exists
  -- filter to OT games: check if OT column exists, e.g. g.period > 4
;
```

**Confirmed success:** Row 182.

---

## RULE 11 — Consistency Metrics: Always Weight by Sample Size

Low standard deviation alone does not equal consistency. A player with 3 games has trivially low variance. Always incorporate sample size as a confidence multiplier. Derive minimum sample size dynamically — never hardcode.

```sql
WITH stats AS (
    SELECT pb.player_id, p.full_name,
        COUNT(*)        AS games_played,
        AVG(pb.points)  AS avg_pts,
        STDDEV(pb.points) AS std_pts,
        AVG(pb.points) / NULLIF(STDDEV(pb.points), 0) AS cv_ratio
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
    GROUP BY pb.player_id, p.full_name
),
threshold AS (
    SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY games_played) AS min_games
    FROM stats
)
SELECT s.full_name, s.avg_pts, s.std_pts, s.games_played,
    s.cv_ratio * LEAST(s.games_played::numeric / th.min_games, 1) AS weighted_consistency
FROM stats s, threshold th
WHERE s.games_played >= th.min_games
ORDER BY weighted_consistency DESC LIMIT 10;

-- WRONG: lowest STDDEV wins — 3-game players always top the list
SELECT player_id, STDDEV(points) FROM ... ORDER BY STDDEV ASC LIMIT 1;
```

**Confirmed failures fixed:** Rows 39, 126.

---

## RULE 12 — Average by Group (e.g., Minutes by Age): Use AVG Not SUM

When the question asks for an average rate across a grouping dimension, use AVG. Using SUM returns a meaningless total across all players of that age.

```sql
-- CORRECT: average minutes by player age
SELECT EXTRACT(YEAR FROM AGE(g.game_date, p.birthdate))::int AS player_age,
    AVG(pb.minutes_played) AS avg_minutes_per_game
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
WHERE g.game_type ILIKE '%Regular Season%'
GROUP BY player_age ORDER BY player_age;

-- WRONG: SUM gives total minutes, not average rate
SELECT age, SUM(minutes_played) FROM ... GROUP BY age;
```

**Confirmed failure fixed:** Row 127. **Confirmed success:** Row 128.

---

## RULE 13 — Player Efficiency Rating (PER): Do Not Compute If Column Not Present

PER requires league averages, pace adjustment, and normalization that cannot be reliably computed from base boxscore columns. If a `per` or `player_efficiency_rating` column does not exist in the schema, do not attempt to compute it. Label simplified approximations clearly as "Simplified Efficiency Score", not PER.

```sql
-- If PER column exists
SELECT p.full_name, pb.per
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id;

-- If PER column does NOT exist: state clearly that PER is not available
-- Offer simplified efficiency: (pts + reb + ast + stl + blk - tov - missed_fg - missed_ft) / games
```

**Confirmed failure acknowledged:** Row 81 — PER is not reliably computable from available columns.

---

## RULE 14 — Net Rating: Compute From Available Team Boxscore Columns

`net_rating` does not exist as a stored column. Compute it as offensive_rating - defensive_rating.

```sql
AVG(tb.estimated_offensive_rating) - AVG(tb.estimated_defensive_rating) AS net_rating
```

Do not reference `tb.net_rating` directly — column does not exist.

**Confirmed failure fixed:** Row 124.

---

## RULE 15 — Quarter/Period Stats: Do Not Reference Non-Existent Columns

Quarter-by-quarter scoring breakdowns (highest scoring quarter) are not reliably available in the base boxscore schema. If a `quarter_*` column does not exist, state that the data is unavailable rather than referencing a non-existent column.

**Confirmed failure acknowledged:** Row 140 — quarter columns referenced did not exist.

---

## RULE 16 — Season-Over-Season Comparison: Two CTEs with Separate Season Filters

When comparing current vs previous season performance, create two CTEs each with their own season filter using the `season_year` column.

```sql
WITH current_season AS (
    SELECT p.full_name,
        AVG(pb.points)  AS avg_pts,
        AVG(pb.assists) AS avg_ast,
        AVG(pb.rebounds_offensive + pb.rebounds_defensive) AS avg_reb
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Giannis Antetokounmpo%'
      AND g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
    GROUP BY p.full_name
),
last_season AS (
    SELECT p.full_name,
        AVG(pb.points)  AS avg_pts,
        AVG(pb.assists) AS avg_ast,
        AVG(pb.rebounds_offensive + pb.rebounds_defensive) AS avg_reb
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Giannis Antetokounmpo%'
      AND g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (
          SELECT MAX(season_year) FROM dwh_d_games
          WHERE season_year < (SELECT MAX(season_year) FROM dwh_d_games)
            AND game_type ILIKE '%Regular Season%')
    GROUP BY p.full_name
)
SELECT cs.full_name,
    cs.avg_pts AS current_ppg, ls.avg_pts AS last_ppg,
    cs.avg_pts - ls.avg_pts   AS ppg_change
FROM current_season cs JOIN last_season ls USING (full_name);
```

**Confirmed successes:** Rows 157, 169, 186, 187.

---

## RULE 17 — season_year Type Safety: Cast to Integer for Arithmetic

`season_year` is stored as a text/varchar column in `dwh_d_games`. Any arithmetic operation on it requires an explicit cast.

```sql
-- CORRECT
WHERE g.season_year::integer >= (SELECT MAX(season_year)::integer FROM dwh_d_games) - 9

-- WRONG: type error on text - integer arithmetic
WHERE g.season_year >= MAX(season_year) - 9
```

---

## RULE 18 — Safe Division: Always NULLIF on Any Denominator

Every division must use NULLIF to guard against division by zero.

```sql
SUM(pb.points)::numeric / NULLIF(SUM(pb.field_goals_attempted), 0)
COUNT(wins)::numeric / NULLIF(COUNT(*), 0)
SUM(pb.assists)::numeric / NULLIF(SUM(pb.turnovers), 0)
```

---

## RULE 19 — Playoff Teams in a Season

```sql
SELECT DISTINCT t.full_name AS team_name
FROM dwh_d_games g
JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
WHERE g.season_year = '2022'
  AND g.game_type ILIKE '%playoff%'
ORDER BY t.full_name;
```

**Confirmed success:** Row 177.

---

## Anti-Pattern Summary

| Bad Pattern | Consequence | Fix |
|---|---|---|
| `AVG(plus_minus_points)` for ranking | Per-game rate, not total impact | `SUM(plus_minus_points)` |
| `DATE_TRUNC('year', game_date)` grouping | Splits NBA season in two | Use `g.season_year` |
| `AVG(stat/min*36)` for per-36 | Garbage-time games over-weighted | `SUM(stat)/SUM(min)*36` |
| STDDEV only for consistency | 3-game players win | Multiply by sample-size weight |
| Only pts/reb/ast for triple-double | Misses steals/blocks combos | Check all 5 categories |
| `SUM(minutes)` by age group | Total, not average | `AVG(minutes_played)` |
| `season_year - 9` on text field | Type error | `season_year::integer - 9` |
| Division without NULLIF | Crash on zero denominator | `/ NULLIF(denominator, 0)` |
| `net_rating` column reference | Column doesn't exist | Compute `off_rating - def_rating` |
| `quarter_*` column reference | Column may not exist | State data unavailable |
| Season filter on career query | Excludes prior seasons | Remove season_year filter |
