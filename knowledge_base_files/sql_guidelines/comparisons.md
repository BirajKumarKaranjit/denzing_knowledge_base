---
name: comparisons
description: "Use for CASE WHEN expressions, playoff round ordering, side-by-side player comparisons, head-to-head matchup stats, win/loss records when scoring a threshold, player/team ranking, conference comparisons, season-over-season comparisons, column existence validation, and back-to-back performance checks."
tags: [comparisons, CASE WHEN, playoff rounds, column validation, head-to-head, win/loss threshold, ranking, conference, season-over-season, back-to-back, RANK]
priority: critical
---

# SQL Comparisons Guide

---

## RULE 1 — Playoff Round Ordering: Map Strings to Integers

Playoff rounds are strings. `MAX(playoff_round)` or `ORDER BY playoff_round` uses alphabetical order which is wrong. Always map to integers first.

```sql
CASE playoff_round
    WHEN 'First Round'           THEN 1
    WHEN 'Conference Semifinals' THEN 2
    WHEN 'Conference Finals'     THEN 3
    WHEN 'Finals'                THEN 4
    ELSE 0
END AS round_num
-- Then MAX(round_num) or ORDER BY round_num DESC
```

---

## RULE 2 — Column Existence: Only Reference Confirmed Columns

| Wrong (do not use) | Correct |
|---|---|
| `rebounds_chances_offensive` | `rebounds_offensive` |
| `rebounds_chances_defensive` | `rebounds_defensive` |
| `net_rating` | Compute: `AVG(estimated_offensive_rating) - AVG(estimated_defensive_rating)` |
| `per` / `player_efficiency_rating` | Check schema; if absent, do not compute |
| `quarter_1_points`, `quarter_*` | Check schema; if absent, state data unavailable |

Before referencing any derived metric, verify it exists. If it doesn't — compute from base columns or clearly state unavailability. Never guess a column name.

---

## RULE 3 — Win/Loss When Scoring Threshold: Use pb.team_id

```sql
WITH qualifying AS (
    SELECT pb.team_id, g.home_team_id, g.visitor_team_id, g.home_score, g.visitor_score
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Kevin Durant%' AND pb.points >= 30
      AND g.game_type ILIKE '%Regular Season%'
)
SELECT
    SUM(CASE WHEN (team_id = home_team_id    AND home_score    > visitor_score) OR
                  (team_id = visitor_team_id AND visitor_score > home_score) THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN (team_id = home_team_id    AND home_score    < visitor_score) OR
                  (team_id = visitor_team_id AND visitor_score < home_score) THEN 1 ELSE 0 END) AS losses,
    ROUND(wins::numeric / NULLIF(COUNT(*), 0) * 100, 1) AS win_pct
FROM qualifying;
```

---

## RULE 4 — Player Ranking: RANK() Window Function

```sql
SELECT p.full_name, SUM(pb.points) AS total_points,
    RANK() OVER (ORDER BY SUM(pb.points) DESC) AS rank
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
WHERE g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name ORDER BY total_points DESC;
```

---

## RULE 5 — Conference Comparison: Use Stored Values 'East' / 'West'

```sql
-- Wins by conference
SELECT t.conference, COUNT(*) AS wins
FROM dwh_d_games g
JOIN dwh_d_teams t ON
    CASE WHEN g.home_score > g.visitor_score THEN g.home_team_id ELSE g.visitor_team_id END = t.team_id
WHERE g.game_type ILIKE '%Regular Season%' AND t.conference IN ('East', 'West')
GROUP BY t.conference;
```

---

## RULE 6 — Side-by-Side Comparison: Separate CTEs Per Player

```sql
WITH player1 AS (
    SELECT p.full_name, AVG(pb.points) AS ppg, AVG(pb.assists) AS apg
    FROM ... WHERE p.full_name ILIKE '%Giannis%' ...
),
player2 AS (
    SELECT p.full_name, AVG(pb.points) AS ppg, AVG(pb.assists) AS apg
    FROM ... WHERE p.full_name ILIKE '%Joel Embiid%' ...
)
SELECT * FROM player1 UNION ALL SELECT * FROM player2;
```

---

## RULE 7 — Head-to-Head in Same Games: pb1.player_id < pb2.player_id

Prevents duplicate rows (A vs B and B vs A appearing twice).

```sql
JOIN dwh_f_player_boxscore pb2 ON pb1.game_id = pb2.game_id
    AND pb1.player_id < pb2.player_id
```

---

## RULE 8 — Back-to-Back: LAG on Full Game Sequence, Never Pre-Filter

```sql
WITH all_games AS (
    SELECT pb.player_id, g.game_date, pb.points,
        LAG(g.game_date) OVER (PARTITION BY pb.player_id ORDER BY g.game_date) AS prev_date,
        LAG(pb.points)   OVER (PARTITION BY pb.player_id ORDER BY g.game_date) AS prev_points
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%Kawhi Leonard%' AND g.game_type ILIKE '%Regular Season%'
)
SELECT game_date, points, prev_date, prev_points
FROM all_games WHERE points >= 40 AND prev_points >= 40 AND game_date - prev_date <= 2;

-- WRONG: filtering 40+ first removes gaps and makes logic invalid
```

---

## RULE 9 — Conditional Aggregation: FILTER or CASE WHEN

```sql
SELECT p.full_name,
    COUNT(*) FILTER (WHERE pb.points >= 30)               AS games_30_plus,
    SUM(pb.points) FILTER (WHERE g.game_type ILIKE '%playoff%') AS playoff_pts
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
WHERE p.full_name ILIKE '%LeBron James%'
GROUP BY p.full_name;
```

---

## Anti-Pattern Summary

| Bad Pattern | Fix |
|---|---|
| `MAX(playoff_round)` on string | Map to integers, then MAX |
| `rebounds_chances_offensive` | `rebounds_offensive` |
| `net_rating` column | Compute `off_rating - def_rating` |
| `per` column reference | Check schema; don't compute if absent |
| Pre-filter before back-to-back LAG | Full sequence + LAG |
| `= 'Eastern Conference'` | `IN ('East', 'West')` |
| H2H without `pb1.player_id < pb2.player_id` | Duplicate rows |
