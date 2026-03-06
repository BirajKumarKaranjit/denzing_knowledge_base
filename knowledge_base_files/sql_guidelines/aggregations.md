---
name: aggregations
description: "Use for totals, averages, per-game rates, per-36 stats, shooting percentages (FG%, eFG%, TS%, 3P%, FT%), assist-to-turnover ratio, win percentage, season grouping, triple-double/double-double detection, plus-minus ranking, career stats, season stats, consistency metrics, or any GROUP BY aggregation. Contains all standard NBA efficiency formulas and SUM vs AVG decision rules."
tags: [aggregations, SUM, AVG, per-game, per-36, FG%, eFG%, TS%, win percentage, triple-double, double-double, plus-minus, season grouping, season_year, NULLIF, career totals]
priority: critical
---

# SQL Aggregation Guidelines

---

## RULE 1 — SUM vs AVG: Match the User's Intent

| User Phrase | Use |
|---|---|
| "total", "career", "how many overall" | `SUM` |
| "per game", "average", "averaging" | `AVG` |
| "per 36 minutes" | Weighted — see Rule 4 |
| "ranking by plus-minus", "best plus-minus" | `SUM` (total impact, not per-game rate) |
| "average minutes by age" | `AVG` (rate per group, not total) |

---

## RULE 2 — Season Grouping: g.season_year Always, Never DATE_TRUNC('year')

NBA seasons span two calendar years. `DATE_TRUNC('year')` splits the 2024-25 season into two groups. Always group by `g.season_year`.

```sql
-- CORRECT
GROUP BY g.season_year

-- WRONG: Oct-Dec 2024 and Jan-Jun 2025 become separate rows
GROUP BY DATE_TRUNC('year', g.game_date)
```

---

## RULE 3 — Career / All-Time: No Season Filter

For "career", "all-time", "ever", "career high", "how many total" — omit the `season_year` filter entirely.

```sql
SELECT p.full_name, SUM(pb.points) AS career_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
WHERE p.full_name ILIKE '%LeBron James%'
GROUP BY p.full_name;
```

---

## RULE 4 — Per-36 Minutes: Weighted SUM/SUM, Not AVG of Rates

```sql
SUM(pb.points)  / NULLIF(SUM(pb.minutes_played), 0) * 36 AS pts_per_36
SUM(pb.assists) / NULLIF(SUM(pb.minutes_played), 0) * 36 AS ast_per_36
-- Filter: AND pb.minutes_played > 0
```

Never use `AVG(points / minutes * 36)` — garbage-time games get equal weight.

---

## RULE 5 — Shooting Efficiency Formulas

Always compute from base columns. Never assume pre-computed columns exist.

```sql
-- FG%
SUM(pb.field_goals_made)::numeric / NULLIF(SUM(pb.field_goals_attempted), 0) AS fg_pct

-- 3P%
SUM(pb.three_pointers_made)::numeric / NULLIF(SUM(pb.three_pointers_attempted), 0) AS three_pt_pct

-- FT%
SUM(pb.free_throws_made)::numeric / NULLIF(SUM(pb.free_throws_attempted), 0) AS ft_pct

-- eFG%
SUM(pb.field_goals_made + 0.5 * pb.three_pointers_made)::numeric
    / NULLIF(SUM(pb.field_goals_attempted), 0) AS efg_pct

-- TS%
SUM(pb.points)::numeric
    / NULLIF(2 * (SUM(pb.field_goals_attempted) + 0.44 * SUM(pb.free_throws_attempted)), 0) AS ts_pct

-- A/TO Ratio
SUM(pb.assists)::numeric / NULLIF(SUM(pb.turnovers), 0) AS ast_to_tov

-- Net Rating (team boxscore)
AVG(tb.estimated_offensive_rating) - AVG(tb.estimated_defensive_rating) AS net_rating

-- Win %
ROUND(SUM(wins)::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS win_pct
```

---

## RULE 6 — Safe Division: Always NULLIF on Denominators

```sql
SUM(pb.points)::numeric / NULLIF(SUM(pb.field_goals_attempted), 0)
COUNT(wins)::numeric / NULLIF(COUNT(*), 0)
```

---

## RULE 7 — Triple-Double / Double-Double: Check All 5 Categories

Check points, total rebounds, assists, steals, and blocks — not just pts/reb/ast.

```sql
(CASE WHEN pb.points >= 10 THEN 1 ELSE 0 END
 + CASE WHEN (pb.rebounds_offensive + pb.rebounds_defensive) >= 10 THEN 1 ELSE 0 END
 + CASE WHEN pb.assists >= 10 THEN 1 ELSE 0 END
 + CASE WHEN pb.steals  >= 10 THEN 1 ELSE 0 END
 + CASE WHEN pb.blocks  >= 10 THEN 1 ELSE 0 END) >= 3  -- triple-double
-- >= 2 for double-double, >= 4 for quadruple-double
```

---

## RULE 8 — Consistency: Weight by Sample Size

Low STDDEV alone picks players with 3 games. Always apply a sample-size multiplier.

```sql
WITH stats AS (
    SELECT pb.player_id, p.full_name, COUNT(*) AS games,
        AVG(pb.points) / NULLIF(STDDEV(pb.points), 0) AS cv_ratio
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
    GROUP BY pb.player_id, p.full_name
),
threshold AS (SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY games) AS min_games FROM stats)
SELECT full_name, cv_ratio * LEAST(games::numeric / min_games, 1) AS weighted_consistency
FROM stats, threshold WHERE games >= min_games
ORDER BY weighted_consistency DESC;
```

---

## RULE 9 — Season-Over-Season: Two CTEs with Separate Season Filters

```sql
WITH current_szn AS (
    SELECT p.full_name, AVG(pb.points) AS ppg
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Giannis%' AND g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
    GROUP BY p.full_name
),
prev_szn AS (
    -- same query with previous season filter
    ...
)
SELECT cs.full_name, cs.ppg AS current_ppg, ps.ppg AS last_ppg
FROM current_szn cs JOIN prev_szn ps USING (full_name);
```

---

## RULE 10 — season_year Type Safety: Cast Before Arithmetic

`season_year` is stored as text. Cast it before arithmetic.

```sql
-- CORRECT
WHERE g.season_year::integer >= (SELECT MAX(season_year)::integer FROM dwh_d_games) - 9

-- WRONG: type error
WHERE g.season_year >= MAX(season_year) - 9
```

---

## RULE 11 — Average by Group: AVG Not SUM

For "average minutes by age" or "average stat by season" — use `AVG`, not `SUM`.

```sql
SELECT EXTRACT(YEAR FROM AGE(g.game_date, p.birthdate))::int AS age,
    AVG(pb.minutes_played) AS avg_minutes
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
GROUP BY age ORDER BY age;
```

---

## RULE 12 — PER and Quarter Stats: Do Not Reference If Column Absent

PER cannot be reliably computed from base columns — do not attempt it. Quarter-by-quarter breakdowns may not exist. If a column is absent, state that clearly rather than guessing a name.

---

## Anti-Pattern Summary

| Bad Pattern | Fix |
|---|---|
| `AVG(plus_minus)` for ranking | `SUM(plus_minus_points)` |
| `DATE_TRUNC('year')` for season groups | `g.season_year` |
| `AVG(stat/min*36)` for per-36 | `SUM(stat)/SUM(min)*36` |
| STDDEV only for consistency | Multiply by sample-size weight |
| Only pts/reb/ast for triple-double | Check all 5 categories |
| `SUM(minutes)` by age group | `AVG(minutes_played)` |
| `season_year - 9` on text field | `season_year::integer - 9` |
| No NULLIF on denominator | `/ NULLIF(denominator, 0)` |
| `net_rating` column reference | Compute `off_rating - def_rating` |
| Season filter on career query | Remove `season_year` filter |
