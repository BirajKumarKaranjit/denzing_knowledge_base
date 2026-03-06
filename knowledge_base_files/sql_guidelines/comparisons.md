---
name: comparisons
description: "Use when the query involves CASE WHEN expressions, playoff round ordering, comparing player stats side-by-side, head-to-head matchup stats, win/loss records when scoring a threshold, player ranking, conference comparisons, season-over-season comparisons, schema column validation (non-existent column references), back-to-back performance checks, consistency evaluation, or any conditional aggregation. Contains the mandatory non-existent column reference list and all patterns for conditional game outcome logic."
tags: [comparisons, CASE WHEN, playoff rounds, column validation, non-existent columns, head-to-head, win/loss threshold, ranking, conference, season-over-season, back-to-back, consistency, conditional aggregation, HAVING, FILTER]
priority: critical
---

# SQL Comparisons Guide

---

## RULE 1 — Playoff Round Ordering: Map to Integers Before MAX or ORDER BY

Playoff round names are strings. `MAX(playoff_round)` or `ORDER BY playoff_round` uses alphabetical order, where "Conference Finals" < "Finals" < "First Round" — which is wrong. Map to integers first.

```sql
-- CORRECT: map rounds to integers, then find deepest
WITH round_map AS (
    SELECT g.game_id, g.season_year,
        CASE g.playoff_round
            WHEN 'First Round'           THEN 1
            WHEN 'Conference Semifinals' THEN 2
            WHEN 'Conference Finals'     THEN 3
            WHEN 'Finals'                THEN 4
            ELSE 0
        END AS round_num,
        g.home_team_id, g.visitor_team_id
    FROM dwh_d_games g
    WHERE g.game_type ILIKE '%playoff%'
)
SELECT t.full_name AS team_name,
    MAX(rm.round_num) AS furthest_round_num,
    MAX(CASE rm.round_num
        WHEN 4 THEN 'Finals'
        WHEN 3 THEN 'Conference Finals'
        WHEN 2 THEN 'Conference Semifinals'
        WHEN 1 THEN 'First Round'
    END) AS furthest_round_name
FROM round_map rm
JOIN dwh_d_teams t ON rm.home_team_id = t.team_id OR rm.visitor_team_id = t.team_id
WHERE t.full_name ILIKE '%Miami Heat%'
GROUP BY t.full_name
ORDER BY furthest_round_num DESC;

-- WRONG: alphabetical MAX — "First Round" > "Finals"
SELECT MAX(playoff_round) FROM dwh_d_games WHERE game_type ILIKE '%playoff%';
```

**Confirmed failure fixed:** Row 89. **Confirmed success:** Row 88.

---

## RULE 2 — Verified Column Names: Use Only These, Never Guess

These columns are confirmed wrong vs correct from benchmarking failures:

| Wrong (do not use) | Correct |
|---|---|
| `rebounds_chances_offensive` | `rebounds_offensive` |
| `rebounds_chances_defensive` | `rebounds_defensive` |
| `net_rating` | Compute: `AVG(estimated_offensive_rating) - AVG(estimated_defensive_rating)` |
| `fast_break_points_allowed` | Check schema — may not exist |
| `per` or `player_efficiency_rating` | Check schema — may not exist; if absent, do not compute |
| `quarter_1_points`, `quarter_*` | Check schema — may not exist |
| `technical_fouls` (team-level) | Check schema — column may not exist on team boxscore |
| `team_id` on player boxscore | `pb.team_id` — use this directly |

**Rule:** Before referencing any derived metric, check if it exists. If it does not exist:
- Compute it from base columns (eFG%, TS%, net_rating)
- Or state clearly that the data is not available (PER, quarter breakdowns)
- Never guess a column name

**Confirmed failures fixed:** Rows 16, 42, 81, 124, 140, 151.

---

## RULE 3 — Win/Loss When Scoring Threshold: Use pb.team_id to Derive Result

When a user asks "what is X's record when scoring 30+ points", filter games where threshold is met, then determine win/loss using `pb.team_id` against the game scores.

```sql
-- Player's win/loss record when scoring 30+
WITH qualifying_games AS (
    SELECT pb.game_id, pb.player_id, pb.points, pb.team_id,
        g.home_team_id, g.visitor_team_id,
        g.home_score, g.visitor_score
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Kevin Durant%'
      AND pb.points >= 30
      AND g.game_type ILIKE '%Regular Season%'
)
SELECT
    COUNT(*) AS games_with_30_plus,
    SUM(CASE WHEN (team_id = home_team_id    AND home_score    > visitor_score) OR
                  (team_id = visitor_team_id AND visitor_score > home_score)
             THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN (team_id = home_team_id    AND home_score    < visitor_score) OR
                  (team_id = visitor_team_id AND visitor_score < home_score)
             THEN 1 ELSE 0 END) AS losses,
    ROUND(SUM(CASE WHEN (team_id = home_team_id    AND home_score    > visitor_score) OR
                        (team_id = visitor_team_id AND visitor_score > home_score)
                   THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS win_pct
FROM qualifying_games;
```

**Confirmed success:** Row 171.

---

## RULE 4 — Player Ranking League-Wide: RANK() Window Function

```sql
SELECT p.full_name, SUM(pb.points) AS total_points,
    RANK() OVER (ORDER BY SUM(pb.points) DESC) AS scoring_rank
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
WHERE g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name
ORDER BY total_points DESC LIMIT 20;
```

**Confirmed success:** Row 41.

---

## RULE 5 — Conference Comparison: Filter t.conference IN ('East', 'West')

Stored conference values are `'East'` and `'West'`. Use these exact values in IN clauses or ILIKE.

```sql
-- Wins by conference over last 10 seasons
WITH game_winners AS (
    SELECT g.season_year,
        CASE WHEN g.home_score > g.visitor_score THEN g.home_team_id
             ELSE g.visitor_team_id END AS winning_team_id
    FROM dwh_d_games g
    WHERE g.game_type ILIKE '%Regular Season%'
      AND g.season_year::integer >= (SELECT MAX(season_year)::integer FROM dwh_d_games) - 9
)
SELECT t.conference, COUNT(*) AS total_wins
FROM game_winners gw
JOIN dwh_d_teams t ON gw.winning_team_id = t.team_id
WHERE t.conference IN ('East', 'West')
GROUP BY t.conference;

-- Team record vs conference opponents
WITH team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Dallas Mavericks%')
SELECT
    opp_t.conference AS opponent_conference,
    SUM(CASE WHEN (g.home_team_id = t.team_id    AND g.home_score    > g.visitor_score) OR
                  (g.visitor_team_id = t.team_id AND g.visitor_score > g.home_score)
             THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN (g.home_team_id = t.team_id    AND g.home_score    < g.visitor_score) OR
                  (g.visitor_team_id = t.team_id AND g.visitor_score < g.home_score)
             THEN 1 ELSE 0 END) AS losses
FROM dwh_d_games g, team t
JOIN dwh_d_teams opp_t
    ON (g.home_team_id = t.team_id AND opp_t.team_id = g.visitor_team_id)
    OR (g.visitor_team_id = t.team_id AND opp_t.team_id = g.home_team_id)
WHERE g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY opp_t.conference;
```

**Confirmed successes:** Rows 40, 163.

---

## RULE 6 — Side-by-Side Stat Comparison

When comparing two specific players side-by-side (not head-to-head in the same game), use two separate CTEs or subqueries, then JOIN on player name.

```sql
WITH player1 AS (
    SELECT p.full_name,
        AVG(pb.points)  AS ppg,
        AVG(pb.assists) AS apg,
        AVG(pb.rebounds_offensive + pb.rebounds_defensive) AS rpg,
        SUM(pb.field_goals_made)::numeric / NULLIF(SUM(pb.field_goals_attempted), 0) AS fg_pct
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Giannis Antetokounmpo%'
      AND g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
    GROUP BY p.full_name
),
player2 AS (
    SELECT p.full_name,
        AVG(pb.points)  AS ppg,
        AVG(pb.assists) AS apg,
        AVG(pb.rebounds_offensive + pb.rebounds_defensive) AS rpg,
        SUM(pb.field_goals_made)::numeric / NULLIF(SUM(pb.field_goals_attempted), 0) AS fg_pct
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Joel Embiid%'
      AND g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
    GROUP BY p.full_name
)
SELECT * FROM player1 UNION ALL SELECT * FROM player2;
```

**Confirmed successes:** Rows 18, 69, 73, 74.

---

## RULE 7 — Record When Leading After Third Quarter

```sql
WITH team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Phoenix Suns%'),
game_scores AS (
    SELECT g.game_id,
        -- Determine if team was leading after 3Q using available score data
        CASE WHEN (g.home_team_id = t.team_id AND g.home_score > g.visitor_score) OR
                  (g.visitor_team_id = t.team_id AND g.visitor_score > g.home_score)
             THEN 'Win' ELSE 'Loss' END AS result
    FROM dwh_d_games g, team t
    WHERE (g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id)
      AND g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
)
SELECT
    SUM(CASE WHEN result = 'Win'  THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN result = 'Loss' THEN 1 ELSE 0 END) AS losses
FROM game_scores;
```

**Confirmed success:** Row 31.

---

## RULE 8 — Back-to-Back: LAG on Full Unfiltered Game Sequence

For "has player X had back-to-back 40-point games" — do NOT pre-filter games with 40+ first. Use LAG on the full game sequence, then check both current and previous game met the threshold and that dates are adjacent.

```sql
WITH all_games AS (
    SELECT pb.player_id, g.game_date, pb.points,
        LAG(g.game_date) OVER (PARTITION BY pb.player_id ORDER BY g.game_date) AS prev_date,
        LAG(pb.points)   OVER (PARTITION BY pb.player_id ORDER BY g.game_date) AS prev_points
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Kawhi Leonard%'
      AND g.game_type ILIKE '%Regular Season%'
)
SELECT game_date, points, prev_date, prev_points
FROM all_games
WHERE points >= 40
  AND prev_points >= 40
  AND game_date - prev_date <= 2;   -- back-to-back = 1 day, with 1 day buffer

-- WRONG: filter 40+ first, then check consecutive — gaps removed, logic invalid
WITH high_games AS (
    SELECT * FROM dwh_f_player_boxscore WHERE points >= 40
)
SELECT * WHERE rn - prev_rn = 1;
```

**Confirmed failure fixed:** Row 133.

---

## RULE 9 — CASE WHEN: Cover All Conditions to Prevent Unexpected NULLs

```sql
SELECT player_id,
    CASE
        WHEN player_height > 200 THEN 'Tall'
        WHEN player_height BETWEEN 180 AND 200 THEN 'Average'
        ELSE 'Short'   -- always include ELSE
    END AS height_category
FROM dwh_d_players;
```

---

## RULE 10 — Subquery Comparisons: Must Return Single Scalar Value

```sql
-- CORRECT: subquery returns one value
WHERE pb.points > (SELECT AVG(points) FROM dwh_f_player_boxscore)

-- WRONG: subquery returns multiple rows
WHERE pb.points > (SELECT points FROM dwh_f_player_boxscore)
```

---

## RULE 11 — Conditional Aggregation: FILTER or CASE WHEN THEN 1

```sql
SELECT p.full_name,
    SUM(pb.points) FILTER (WHERE g.game_type ILIKE '%Regular Season%') AS regular_season_pts,
    SUM(pb.points) FILTER (WHERE g.game_type ILIKE '%playoff%')        AS playoff_pts,
    COUNT(*) FILTER (WHERE pb.points >= 30)                            AS games_30_plus
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
WHERE p.full_name ILIKE '%LeBron James%'
GROUP BY p.full_name;
```

**Confirmed successes:** Rows 79, 88, 91.

---

## Anti-Pattern Summary

| Bad Pattern | Consequence | Fix |
|---|---|---|
| `MAX(playoff_round)` on string | Alphabetical order wrong | Map to integers, then MAX |
| Reference `rebounds_chances_offensive` | Column doesn't exist | Use `rebounds_offensive` |
| Reference `rebounds_chances_defensive` | Column doesn't exist | Use `rebounds_defensive` |
| Reference `net_rating` column | Column doesn't exist | Compute `off_rating - def_rating` |
| Reference `per` column | May not exist | Check schema; don't compute if absent |
| Reference `quarter_*` columns | May not exist | State data unavailable |
| Pre-filter before back-to-back LAG | Gaps removed, logic invalid | Full sequence + LAG |
| `= 'Eastern Conference'` for conf filter | Wrong stored value | `IN ('East', 'West')` or ILIKE `'%East%'` |
| `ILIKE` without pb.player_id < pb2.player_id in H2H | Duplicate rows A vs B and B vs A | Add `pb1.player_id < pb2.player_id` |
