---
name: joins
description: "Use when the query involves linking tables to retrieve related data — player names, team names, game matchups, player stats on a specific team, opponent identification, win/loss records, championships, teams a player has played for, or back-to-back team schedules. Critical rules: always use pb.team_id to identify a player's team (never the game table), always show readable names in output (never raw IDs), use correct opponent join pattern, resolve win/loss from score columns not plus-minus, use dwh_f_team_championships for championship data, use dwh_f_player_team_seasons for teams-played-for queries."
tags: [joins, team names, player names, output formatting, pb.team_id, team stats, opponent join, win/loss, championships, teams played for, LEFT JOIN, back-to-back, double join, home visitor, raw IDs]
priority: critical
---

# SQL Join Guidelines

---

## RULE 1 — Always Show Readable Names: Never Expose Raw IDs in Output

Every query result must show human-readable names, not numeric IDs, abbreviations, or surrogate keys.

```sql
-- CORRECT: user sees readable names
SELECT p.full_name AS player_name,
       t.full_name AS team_name,
       g.game_date,
       pb.points, pb.assists,
       pb.rebounds_offensive + pb.rebounds_defensive AS total_rebounds
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_teams t   ON pb.team_id   = t.team_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id;

-- WRONG: user sees numeric IDs
SELECT pb.player_id, pb.team_id, pb.game_id, pb.points FROM dwh_f_player_boxscore pb;
```

**Output checklist before writing any SELECT:**
- `player_id` in output? → `JOIN dwh_d_players p` → select `p.full_name`
- `team_id` in output? → `JOIN dwh_d_teams t` → select `t.full_name`
- `game_id` with no date? → `JOIN dwh_d_games g` → select `g.game_date`
- Home team? → `JOIN dwh_d_teams ht ON g.home_team_id = ht.team_id` → select `ht.full_name`
- Visitor team? → `JOIN dwh_d_teams vt ON g.visitor_team_id = vt.team_id` → select `vt.full_name`

**Confirmed failure fixed:** Row 72 (showed team_id instead of team name).

---

## RULE 2 — Player Team Identity: ALWAYS pb.team_id, Never Infer From Game Table

**This is the most severe reliability bug.** When finding stats for players on a specific team, filter using `pb.team_id`. Do NOT join game to teams via `g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id` and then pull player stats — that includes ALL players in those games, including the opposing team's players.

```sql
-- CORRECT: pb.team_id directly identifies which team the player played for
WITH target_team AS (
    SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Timberwolves%'
)
SELECT p.full_name, SUM(pb.points) AS total_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
JOIN target_team t   ON pb.team_id   = t.team_id   -- pb.team_id, not game teams
WHERE g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name ORDER BY total_points DESC LIMIT 1;

-- WRONG: OR join on game teams includes opponent players in stats
WITH target_games AS (
    SELECT g.game_id FROM dwh_d_games g
    JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
    WHERE t.full_name ILIKE '%Timberwolves%'
)
SELECT pb.player_id, SUM(pb.points)    -- counts Timberwolves AND their opponents!
FROM dwh_f_player_boxscore pb
WHERE pb.game_id IN (SELECT game_id FROM target_games)
GROUP BY pb.player_id;
```

**Confirmed failure fixed:** Rows 120, 156. **Confirmed successes:** Rows 117, 132, 204, 205.

---

## RULE 3 — Player Plus-Minus By Team: Filter pb.team_id, Not Career Stats

When a user asks "what is X's plus-minus on the Lakers" or "best plus-minus players on Y team", filter `pb.team_id = target_team` to scope to only games played for that team. Do NOT pull career plus-minus totals and filter by team membership in `dwh_f_player_team_seasons`.

```sql
-- CORRECT: only games where player was on the target team
WITH lakers AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Los Angeles Lakers%')
SELECT p.full_name,
       SUM(pb.plus_minus_points) AS total_plus_minus,
       AVG(pb.plus_minus_points) AS avg_plus_minus
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
JOIN lakers l        ON pb.team_id   = l.team_id
WHERE g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY p.full_name
ORDER BY total_plus_minus DESC;

-- WRONG: gets career leader who has EVER played for Lakers, not who has best +/- FOR the Lakers
SELECT player_id, MAX(plus_minus_points)
FROM dwh_f_player_boxscore
WHERE player_id IN (SELECT player_id FROM dwh_f_player_team_seasons WHERE team_id = 'LAL');
```

**Confirmed failures fixed:** Rows 155, 156. **Note:** Player plus-minus = `pb.plus_minus_points` from `dwh_f_player_boxscore`. Team point differential = computed from `dwh_f_team_boxscore`. Do not substitute one for the other.

---

## RULE 4 — Game-Level Team Queries: OR Join Is Correct for Schedule/Score Only

The `OR` join pattern (`g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id`) is correct and appropriate **only when you want all games a team played** — for schedule, scores, win/loss records. It must NOT be used to pull player stats.

```sql
-- CORRECT: find all games a team played (game-level query)
WITH celtics AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Boston Celtics%')
SELECT g.game_id, g.game_date, g.home_score, g.visitor_score
FROM dwh_d_games g
JOIN celtics c ON g.home_team_id = c.team_id OR g.visitor_team_id = c.team_id
WHERE g.game_type ILIKE '%Regular Season%';

-- WRONG: using this pattern to pull player stats includes opponents
-- → use Rule 2 (pb.team_id) for player stats instead
```

---

## RULE 5 — Win/Loss Record: Use Score Columns, Never Plus-Minus

When calculating team wins and losses, use `home_score` and `visitor_score` from `dwh_d_games`. Never infer wins from plus-minus, which reflects individual player impact, not team wins.

```sql
-- CORRECT: season record from scores
WITH team AS (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Golden State Warriors%')
SELECT
    SUM(CASE WHEN (g.home_team_id    = t.team_id AND g.home_score    > g.visitor_score) OR
                  (g.visitor_team_id = t.team_id AND g.visitor_score > g.home_score)
             THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN (g.home_team_id    = t.team_id AND g.home_score    < g.visitor_score) OR
                  (g.visitor_team_id = t.team_id AND g.visitor_score < g.home_score)
             THEN 1 ELSE 0 END) AS losses,
    COUNT(*) AS games_played
FROM dwh_d_games g, team t
WHERE (g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id)
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%');

-- Away record only
SELECT
    SUM(CASE WHEN g.visitor_score > g.home_score THEN 1 ELSE 0 END) AS away_wins,
    SUM(CASE WHEN g.visitor_score < g.home_score THEN 1 ELSE 0 END) AS away_losses
FROM dwh_d_games g
JOIN dwh_d_teams t ON g.visitor_team_id = t.team_id
WHERE t.full_name ILIKE '%Los Angeles Lakers%'
  AND g.game_type ILIKE '%Regular Season%';

-- Home record only
SELECT
    SUM(CASE WHEN g.home_score > g.visitor_score THEN 1 ELSE 0 END) AS home_wins,
    SUM(CASE WHEN g.home_score < g.visitor_score THEN 1 ELSE 0 END) AS home_losses
FROM dwh_d_games g
JOIN dwh_d_teams t ON g.home_team_id = t.team_id
WHERE t.full_name ILIKE '%Phoenix Suns%'
  AND g.game_type ILIKE '%Regular Season%';

-- WRONG: inferring wins from plus-minus
SUM(CASE WHEN pb.plus_minus_points > 0 THEN 1 ELSE 0 END) AS wins   -- WRONG
```

**Confirmed failure fixed:** Row 38. **Confirmed successes:** Rows 31, 40, 47, 86, 95, 98, 106, 119, 165, 170, 171, 173, 178, 182, 187.

---

## RULE 6 — Game Matchup Display: Double Join on dwh_d_teams

When a query shows a game result with both team names and scores, join `dwh_d_teams` twice using different aliases.

```sql
SELECT g.game_date,
    ht.full_name AS home_team,  g.home_score,
    vt.full_name AS visitor_team, g.visitor_score,
    CASE WHEN g.home_score > g.visitor_score THEN ht.full_name
         ELSE vt.full_name END AS winner
FROM dwh_d_games g
JOIN dwh_d_teams ht ON g.home_team_id    = ht.team_id
JOIN dwh_d_teams vt ON g.visitor_team_id = vt.team_id
WHERE (g.home_team_id    = (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Golden State Warriors%')
    OR g.visitor_team_id = (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Golden State Warriors%'))
  AND (g.home_team_id    = (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Los Angeles Lakers%')
    OR g.visitor_team_id = (SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Los Angeles Lakers%'))
ORDER BY g.game_date DESC LIMIT 1;
```

**Confirmed successes:** Rows 67, 80, 95.

---

## RULE 7 — Opponent Join: Use pb.team_id to Derive the Opponent

When calculating a player's stats against a specific opponent, use `pb.team_id` to identify the player's team, then derive the opponent from the other team in the game. Never join both home and visitor to the opponent — that creates duplicate rows.

```sql
-- CORRECT: opponent derived from pb.team_id
SELECT p.full_name, g.game_date,
    opp.full_name AS opponent,
    pb.points, pb.assists,
    CASE WHEN (pb.team_id = g.home_team_id    AND g.home_score    > g.visitor_score) OR
              (pb.team_id = g.visitor_team_id AND g.visitor_score > g.home_score)
         THEN 'Win' ELSE 'Loss' END AS result
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
JOIN dwh_d_teams opp
    ON (pb.team_id = g.home_team_id    AND opp.team_id = g.visitor_team_id)
    OR (pb.team_id = g.visitor_team_id AND opp.team_id = g.home_team_id)
WHERE p.full_name ILIKE '%Kevin Durant%'
  AND opp.full_name ILIKE '%Denver Nuggets%'
  AND g.game_type ILIKE '%Regular Season%';

-- WRONG: OR join on opponent creates duplicate rows
JOIN dwh_d_teams opp ON g.home_team_id = opp.team_id OR g.visitor_team_id = opp.team_id
WHERE opp.full_name ILIKE '%Denver Nuggets%'   -- returns player rows twice
```

**Confirmed successes:** Rows 1, 45, 49, 117, 189.

---

## RULE 8 — Championship Queries: Use dwh_f_team_championships

Championship data lives in `dwh_f_team_championships`. Use `MAX(yearawarded)` for most recent champion. Use `COUNT(*)` for total championships. Join to `dwh_d_teams` for team name.

```sql
-- Most recent NBA champion
SELECT t.full_name AS champion, tc.yearawarded, tc.oppositeteam AS runner_up
FROM dwh_f_team_championships tc
JOIN dwh_d_teams t ON tc.team_id = t.team_id
WHERE tc.yearawarded = (SELECT MAX(yearawarded) FROM dwh_f_team_championships)
LIMIT 1;

-- Total championships for a team
SELECT t.full_name, COUNT(*) AS championships_won
FROM dwh_f_team_championships tc
JOIN dwh_d_teams t ON tc.team_id = t.team_id
WHERE t.full_name ILIKE '%Los Angeles Lakers%'
GROUP BY t.full_name;

-- Championship winner for a specific season
SELECT t.full_name AS champion, tc.yearawarded
FROM dwh_f_team_championships tc
JOIN dwh_d_teams t ON tc.team_id = t.team_id
WHERE tc.yearawarded = '2024';
```

**Confirmed successes:** Rows 152, 174, 175, 206.

---

## RULE 9 — Teams a Player Has Played For: DISTINCT from dwh_f_player_team_seasons

To find all teams a player has played for, use `dwh_f_player_team_seasons` with `DISTINCT`. This is the authoritative source for player-team history.

```sql
SELECT DISTINCT t.full_name AS team_name
FROM dwh_f_player_team_seasons pts
JOIN dwh_d_players p ON pts.player_id = p.player_id
JOIN dwh_d_teams t   ON pts.team_id   = t.team_id
WHERE p.full_name ILIKE '%Donovan Mitchell%'
ORDER BY t.full_name;

-- Most teams played for (across all players)
SELECT p.full_name, COUNT(DISTINCT pts.team_id) AS teams_played_for
FROM dwh_f_player_team_seasons pts
JOIN dwh_d_players p ON pts.player_id = p.player_id
GROUP BY p.full_name
ORDER BY teams_played_for DESC LIMIT 10;
```

**Confirmed successes:** Rows 134, 191.

---

## RULE 10 — Player Not in Current Data: Use LEFT JOIN

When a player may have limited or no current-season data (new player, inactive, G-League), use LEFT JOIN so the player's record still returns even if no team or game data is available.

```sql
SELECT p.full_name, p.position,
    t.full_name AS team_name   -- NULL if no team assigned
FROM dwh_d_players p
LEFT JOIN dwh_d_teams t ON p.team_id = t.team_id
WHERE p.full_name ILIKE '%Cooper Flagg%';
```

**Confirmed failure fixed:** Row 164.

---

## RULE 11 — Back-to-Back Team Schedule: Resolve team_id First, Then LAG

For back-to-back game performance, resolve team_id first, then use LAG on game_date across the full game sequence. Never hardcode team abbreviations in window partition logic.

```sql
WITH celtics AS (
    SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Boston Celtics%'
),
team_schedule AS (
    SELECT g.game_id, g.game_date,
           g.home_team_id, g.visitor_team_id,
           g.home_score, g.visitor_score,
           LAG(g.game_date) OVER (ORDER BY g.game_date) AS prev_game_date
    FROM dwh_d_games g, celtics c
    WHERE (g.home_team_id = c.team_id OR g.visitor_team_id = c.team_id)
      AND g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
),
b2b AS (
    SELECT * FROM team_schedule
    WHERE game_date - prev_game_date = 1
)
SELECT
    COUNT(*) AS b2b_games,
    SUM(CASE WHEN (home_team_id  = (SELECT team_id FROM celtics) AND home_score    > visitor_score) OR
                  (visitor_team_id = (SELECT team_id FROM celtics) AND visitor_score > home_score)
             THEN 1 ELSE 0 END) AS b2b_wins,
    SUM(CASE WHEN (home_team_id  = (SELECT team_id FROM celtics) AND home_score    < visitor_score) OR
                  (visitor_team_id = (SELECT team_id FROM celtics) AND visitor_score < home_score)
             THEN 1 ELSE 0 END) AS b2b_losses
FROM b2b;

-- WRONG: hardcoded abbreviation fails if stored team_id is not 'BOS'
PARTITION BY CASE WHEN g.home_team_id = 'BOS' THEN ...
```

**Confirmed success:** Row 173.

---

## RULE 12 — Team Performance Queries: Use dwh_f_team_boxscore for Team Metrics

For team-level metrics (offensive rating, defensive rating, steals per game, turnovers per game, rebounds per game, FT%, fast break points), use `dwh_f_team_boxscore` — not player boxscore. For league-wide team comparison, aggregate by team.

```sql
-- Team offensive / defensive efficiency
SELECT t.full_name AS team_name,
    AVG(tb.estimated_offensive_rating) AS avg_offensive_rating,
    AVG(tb.estimated_defensive_rating) AS avg_defensive_rating,
    AVG(tb.estimated_offensive_rating) - AVG(tb.estimated_defensive_rating) AS net_rating
FROM dwh_f_team_boxscore tb
JOIN dwh_d_teams t ON tb.team_id = t.team_id
JOIN dwh_d_games g ON tb.game_id = g.game_id
WHERE t.full_name ILIKE '%Boston Celtics%'
  AND g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY t.full_name;

-- League-wide team ranking (e.g., best defensive rating)
SELECT t.full_name, AVG(tb.estimated_defensive_rating) AS avg_def_rating
FROM dwh_f_team_boxscore tb
JOIN dwh_d_teams t ON tb.team_id = t.team_id
JOIN dwh_d_games g ON tb.game_id = g.game_id
WHERE g.game_type ILIKE '%Regular Season%'
  AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
GROUP BY t.full_name
ORDER BY avg_def_rating ASC LIMIT 5;
```

**Confirmed successes:** Rows 7, 25, 26, 35, 71, 76, 112, 141, 154, 158, 162, 197.

---

## RULE 13 — Head-to-Head Player Comparison: Avoid Duplicates

When comparing two players head-to-head in the same games, join on game_id and add `pb1.player_id < pb2.player_id` to avoid duplicate rows (A vs B and B vs A).

```sql
SELECT p1.full_name AS player1, p2.full_name AS player2,
    AVG(pb1.points) AS p1_avg_pts,
    AVG(pb2.points) AS p2_avg_pts,
    AVG(pb1.rebounds_offensive + pb1.rebounds_defensive) AS p1_avg_reb,
    AVG(pb2.rebounds_offensive + pb2.rebounds_defensive) AS p2_avg_reb
FROM dwh_f_player_boxscore pb1
JOIN dwh_f_player_boxscore pb2 ON pb1.game_id = pb2.game_id
    AND pb1.player_id < pb2.player_id   -- prevents A vs B + B vs A duplicates
JOIN dwh_d_players p1 ON pb1.player_id = p1.player_id
JOIN dwh_d_players p2 ON pb2.player_id = p2.player_id
JOIN dwh_d_games g ON pb1.game_id = g.game_id
WHERE p1.full_name ILIKE '%Joel Embiid%'
  AND p2.full_name ILIKE '%Nikola Jokic%'
  AND g.game_type ILIKE '%Regular Season%'
GROUP BY p1.full_name, p2.full_name;
```

**Confirmed success:** Row 130.

---

## Standard Join Templates

### Player boxscore with all readable labels
```sql
SELECT
    p.full_name  AS player_name,
    t.full_name  AS team_name,
    g.game_date,
    ht.full_name AS home_team,
    vt.full_name AS visitor_team,
    pb.points, pb.assists,
    pb.rebounds_offensive + pb.rebounds_defensive AS total_rebounds
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p  ON pb.player_id       = p.player_id
JOIN dwh_d_teams t    ON pb.team_id          = t.team_id
JOIN dwh_d_games g    ON pb.game_id          = g.game_id
JOIN dwh_d_teams ht   ON g.home_team_id      = ht.team_id
JOIN dwh_d_teams vt   ON g.visitor_team_id   = vt.team_id;
```

---

## Anti-Pattern Summary

| Bad Pattern | Consequence | Fix |
|---|---|---|
| `SELECT pb.player_id` in output | Unreadable ID | `JOIN dwh_d_players` → `p.full_name` |
| `SELECT pb.team_id` in output | Unreadable ID | `JOIN dwh_d_teams` → `t.full_name` |
| OR join on game teams for player stats | Includes opponents | Use `pb.team_id` |
| Career +/- then filter by team membership | Wrong scope | Filter `pb.team_id` directly |
| Win/loss from plus_minus_points | Wrong metric | Use `home_score > visitor_score` |
| Hardcoded abbreviation `'BOS'` | Brittle | Lookup from `dwh_d_teams` |
| `rebounds_chances_offensive` | Column doesn't exist | `rebounds_offensive` |
| `rebounds_chances_defensive` | Column doesn't exist | `rebounds_defensive` |
| JOIN on both opponent home/visitor | Duplicate rows | Use `pb.team_id` derivation |
| Direct JOIN `p.team_id` for career teams | Misses traded seasons | Use `dwh_f_player_team_seasons DISTINCT` |
