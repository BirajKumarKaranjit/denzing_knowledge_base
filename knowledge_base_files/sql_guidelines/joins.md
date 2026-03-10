---
name: joins
description: "Use when the query involves linking tables — player names, team names, game matchups, player stats on a specific team, opponent stats, win/loss records, championships, teams a player has played for, or back-to-back schedules. Critical rules: always use pb.team_id to identify a player's team, always show readable names (never raw IDs), resolve win/loss from score columns not plus-minus."
tags: [joins, team names, player names, pb.team_id, win/loss, championships, opponent join, back-to-back, double join, teams played for, LEFT JOIN]
priority: critical
---

# SQL Join Guidelines

---

## RULE 1 — Always Show Readable Names: Never Expose Raw IDs

Before writing any SELECT, replace every raw ID with its readable name:
- `player_id` → `JOIN dwh_d_players p` → `p.full_name`
- `team_id` → `JOIN dwh_d_teams t` → `t.full_name`
- `game_id` only → `JOIN dwh_d_games g` → `g.game_date`
- Home/visitor teams → double join with aliases `ht` and `vt`

```sql
SELECT p.full_name, t.full_name AS team_name, g.game_date, pb.points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_teams   t ON pb.team_id   = t.team_id
JOIN dwh_d_games   g ON pb.game_id   = g.game_id;
```

---

## RULE 2 — Player Team Identity: ALWAYS pb.team_id, Never the Game Table

Filter player stats to a team using `pb.team_id`. The OR join on home/visitor includes opponent players and returns completely wrong results.

```sql
-- CORRECT
WITH target AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Timberwolves%')
SELECT p.full_name, SUM(pb.points) AS total_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
JOIN target t        ON pb.team_id   = t.team_id
WHERE g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name ORDER BY total_points DESC LIMIT 1;

-- WRONG: includes opponent players
JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
WHERE t.full_name ILIKE '%Timberwolves%'
```

---

## RULE 3 — Game-Level OR Join: Only for Schedule/Score Queries

The OR join (`g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id`) is only valid when retrieving game records — scores, dates, win/loss. Never use it to pull player stats.

---

## RULE 4 — Win/Loss Record: Score Columns Only, Never Plus-Minus

```sql
-- Full season record
WITH team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Golden State Warriors%')
SELECT
    SUM(CASE WHEN (g.home_team_id    = t.team_id AND g.home_score    > g.visitor_score) OR
                  (g.visitor_team_id = t.team_id AND g.visitor_score > g.home_score) THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN (g.home_team_id    = t.team_id AND g.home_score    < g.visitor_score) OR
                  (g.visitor_team_id = t.team_id AND g.visitor_score < g.home_score) THEN 1 ELSE 0 END) AS losses
FROM dwh_d_games g, team t
WHERE (g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id)
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%');

-- Home record: JOIN on home_team_id, check home_score > visitor_score
-- Away record: JOIN on visitor_team_id, check visitor_score > home_score
```

---

## RULE 5 — Game Matchup Display: Double Join on dwh_d_teams

```sql
SELECT g.game_date, ht.full_name AS home_team, g.home_score,
    vt.full_name AS visitor_team, g.visitor_score
FROM dwh_d_games g
JOIN dwh_d_teams ht ON g.home_team_id    = ht.team_id
JOIN dwh_d_teams vt ON g.visitor_team_id = vt.team_id;
```

---

## RULE 6 — Opponent Join: Derive from pb.team_id

Direct OR join on opponent creates duplicate rows. Derive the opponent from the player's side.

```sql
JOIN dwh_d_teams opp
    ON (pb.team_id = g.home_team_id    AND opp.team_id = g.visitor_team_id)
    OR (pb.team_id = g.visitor_team_id AND opp.team_id = g.home_team_id)
WHERE opp.full_name ILIKE '%Denver Nuggets%'
```

---

## RULE 7 — Championships: dwh_f_team_championships

```sql
-- Most recent champion
SELECT t.full_name, tc.yearawarded FROM dwh_f_team_championships tc
JOIN dwh_d_teams t ON tc.team_id = t.team_id
WHERE tc.yearawarded = (SELECT MAX(yearawarded) FROM dwh_f_team_championships);

-- Total championships for a team
SELECT COUNT(*) FROM dwh_f_team_championships tc
JOIN dwh_d_teams t ON tc.team_id = t.team_id
WHERE t.full_name ILIKE '%Los Angeles Lakers%';
```

---

## RULE 8 — Teams a Player Has Played For: DISTINCT from dwh_f_player_team_seasons

```sql
SELECT DISTINCT t.full_name AS team_name
FROM dwh_f_player_team_seasons pts
JOIN dwh_d_players p ON pts.player_id = p.player_id
JOIN dwh_d_teams   t ON pts.team_id   = t.team_id
WHERE p.full_name ILIKE '%Donovan Mitchell%';
```

---

## RULE 9 — Player With No Current Team: LEFT JOIN

```sql
SELECT p.full_name, t.full_name AS team_name
FROM dwh_d_players p
LEFT JOIN dwh_d_teams t ON p.team_id = t.team_id
WHERE p.full_name ILIKE '%Cooper Flagg%';
```

---

## RULE 10 — Plus-Minus on a Team: Filter pb.team_id, Not Career Stats

```sql
WITH team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Los Angeles Lakers%')
SELECT p.full_name, SUM(pb.plus_minus_points) AS total_plus_minus
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
JOIN team t          ON pb.team_id   = t.team_id
WHERE g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name ORDER BY total_plus_minus DESC;
```

Note: `pb.plus_minus_points` is the player metric. Team point differential comes from `dwh_f_team_boxscore`. Never substitute one for the other.

---

## RULE 11 — Team Metrics: Use dwh_f_team_boxscore

For team-level stats (offensive/defensive rating, steals, turnovers, FT%, fast break points), use `dwh_f_team_boxscore`, not player boxscore.

```sql
SELECT t.full_name,
    AVG(tb.estimated_offensive_rating) AS off_rating,
    AVG(tb.estimated_defensive_rating) AS def_rating,
    AVG(tb.estimated_offensive_rating) - AVG(tb.estimated_defensive_rating) AS net_rating
FROM dwh_f_team_boxscore tb
JOIN dwh_d_teams t ON tb.team_id = t.team_id
JOIN dwh_d_games g ON tb.game_id = g.game_id
WHERE t.full_name ILIKE '%Boston Celtics%'
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY t.full_name;
```

---

## RULE 12 — Back-to-Back: Resolve team_id First, Then LAG on game_date

Never hardcode team abbreviations in window logic.

```sql
WITH team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Boston Celtics%'),
schedule AS (
    SELECT g.game_id, g.game_date, g.home_team_id, g.visitor_team_id, g.home_score, g.visitor_score,
        LAG(g.game_date) OVER (ORDER BY g.game_date) AS prev_date
    FROM dwh_d_games g, team t
    WHERE (g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id)
      AND g.game_type ILIKE '%Regular Season%'
)
SELECT * FROM schedule WHERE game_date - prev_date = 1;
```

---

## RULE 13 — Head-to-Head: pb1.player_id < pb2.player_id to Prevent Duplicates

```sql
JOIN dwh_f_player_boxscore pb2 ON pb1.game_id = pb2.game_id
    AND pb1.player_id < pb2.player_id
```

---
## Rule: Avoid OR Joins on Multi-FK Relationships

Some tables contain multiple foreign keys referencing the same dimension
(e.g., home_team_id / visitor_team_id, buyer_id / seller_id, sender_id / receiver_id).

Using OR inside a JOIN multiplies rows because the event can match more than one FK, which makes ORDER BY, LIMIT, and window functions unreliable (they operate on rows, not events).

-- Avoid
JOIN dim_table d 
ON fact.fk_a = d.id OR fact.fk_b = d.id
This can produce multiple rows per event.

-- Correct approach
Resolve the entity ID first, then filter the fact table in WHERE:
WITH entity AS (
  SELECT id FROM dim_table WHERE name ILIKE '%entity%'
)
SELECT ...
FROM fact_table f
WHERE f.fk_a = (SELECT id FROM entity)
   OR f.fk_b = (SELECT id FROM entity)
ORDER BY f.event_date DESC
LIMIT 1;

--Principle

Use JOIN for relationships, and WHERE for membership checks when multiple foreign keys reference the same entity.
---

## Anti-Pattern Summary

| Bad Pattern | Fix |
|---|---|
| Raw `player_id` / `team_id` in SELECT | Join dimension table, show `full_name` |
| OR join on game teams for player stats | Use `pb.team_id` |
| Win/loss from `plus_minus_points` | Use `home_score > visitor_score` |
| Hardcoded team abbrev `'BOS'` | Lookup from `dwh_d_teams` |
| `rebounds_chances_offensive` | `rebounds_offensive` |
| OR join for opponent | Derive via `pb.team_id` pattern |
| Career teams from boxscore | Use `dwh_f_player_team_seasons DISTINCT` |
