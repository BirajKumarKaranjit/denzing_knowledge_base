---
name: joins
description: "Use when the query involves linking tables to retrieve related data, showing team names or player names in output, finding stats for players on a specific team, calculating plus-minus by team, determining opponent in a game, filtering team-specific player stats, or any multi-table query. Critical rules: always resolve team names via join, always use pb.team_id to identify a player's team (never the game table), never expose raw IDs in output, use the correct opponent join pattern."
tags: [joins, team names, player names, output formatting, pb.team_id, team stats, opponent join, plus-minus, player team, foreign keys, multi-table, no raw IDs, double join]
priority: critical
---

# SQL Join Guidelines

---

## RULE 1 — Always Resolve Team Names in Output: Never Expose Raw IDs

Every query that mentions a team in its results must join `dwh_d_teams` and show `t.full_name`. Never return a raw `team_id`, `home_team_id`, or `visitor_team_id` to the user. The same applies to `player_id` — always join `dwh_d_players` and show `p.full_name`.

```sql
-- CORRECT: user sees readable names
SELECT p.full_name AS player_name, t.full_name AS team_name, pb.points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_teams   t ON pb.team_id   = t.team_id;

-- WRONG: user sees numeric IDs or abbreviations
SELECT pb.player_id, pb.team_id, pb.points FROM dwh_f_player_boxscore pb;
```

**Output checklist — before finalizing any SELECT:**
- Is `player_id` exposed? → Add `JOIN dwh_d_players` and select `p.full_name`
- Is `team_id` exposed? → Add `JOIN dwh_d_teams` and select `t.full_name`
- Is `game_id` exposed without `game_date`? → Add `JOIN dwh_d_games` and select `g.game_date`

---

## RULE 2 — Player Team Identity: ALWAYS Use pb.team_id

**This is the most common reliability bug in the codebase.** When finding stats for players on a specific team, you must filter using `pb.team_id`. You must NOT join games to teams using `g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id` and then pull player stats — this includes ALL players in matching games, including the opponent team's players.

**The reliability consequence is severe:** Two syntactically valid but structurally different queries for "top scorer on the Timberwolves" return different players (e.g., Anthony Edwards vs Julius Randle) because one approach counts opponent players.

```sql
-- CORRECT: pb.team_id directly identifies the player's team in that game
WITH team AS (
    SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Timberwolves%'
)
SELECT
    p.full_name,
    SUM(pb.points) AS total_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g ON pb.game_id = g.game_id
JOIN team t ON pb.team_id = t.team_id              -- ← pb.team_id, not game home/visitor
WHERE g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
  AND g.game_type ILIKE '%Regular Season%'
GROUP BY p.full_name
ORDER BY total_points DESC
LIMIT 1;

-- WRONG: joins game to teams using OR — collects ALL players in those games including opponents
WITH timberwolves_games AS (
    SELECT g.game_id
    FROM dwh_d_games g
    JOIN dwh_d_teams t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
    WHERE t.full_name ILIKE '%Timberwolves%'
)
SELECT pb.player_id, SUM(pb.points)
FROM dwh_f_player_boxscore pb
WHERE pb.game_id IN (SELECT game_id FROM timberwolves_games)  -- includes opponent players!
GROUP BY pb.player_id;
```

---

## RULE 3 — Game Table Team Join: Only for Game-Level Queries

The `OR` join pattern (`g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id`) is valid ONLY when you are querying game-level records (win/loss record, schedule, scores) — not when you need player stats filtered to a team.

```sql
-- CORRECT use of OR join: finding a team's games (game records only)
WITH team AS (
    SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Boston Celtics%'
)
SELECT g.game_id, g.game_date, g.home_score, g.visitor_score
FROM dwh_d_games g
JOIN team t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
WHERE g.game_type ILIKE '%Regular Season%';

-- WRONG: using the same OR join to then get player stats — DO NOT DO THIS
-- It pulls opponent players into the player stats result
```

---

## RULE 4 — Never Hardcode Team Abbreviations in Join Logic

When filtering games for a team using window functions or LAG patterns, never hardcode the team abbreviation (e.g., `'BOS'`). Always look up the `team_id` from `dwh_d_teams` first.

```sql
-- CORRECT: look up team_id dynamically
WITH celtics AS (
    SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Boston Celtics%'
),
celtics_games AS (
    SELECT g.game_id, g.game_date,
        LAG(g.game_date) OVER (ORDER BY g.game_date) AS prev_game_date
    FROM dwh_d_games g
    JOIN celtics c ON g.home_team_id = c.team_id OR g.visitor_team_id = c.team_id
    WHERE g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
)
...

-- WRONG: hardcoded abbreviation fails if stored team_id is not 'BOS'
PARTITION BY CASE WHEN g.home_team_id = 'BOS' THEN g.home_team_id ELSE g.visitor_team_id END
```

---

## RULE 5 — Opponent Identification: Use pb.team_id to Derive Opponent

When calculating a player's stats against a specific opponent, derive the opponent from the player's own team (`pb.team_id`). Do NOT join on `g.home_team_id = opp.team_id OR g.visitor_team_id = opp.team_id` — this creates duplicate rows.

```sql
-- CORRECT: derive opponent using pb.team_id
JOIN dwh_d_teams opp
  ON (pb.team_id = g.home_team_id    AND opp.team_id = g.visitor_team_id)
  OR (pb.team_id = g.visitor_team_id AND opp.team_id = g.home_team_id)
WHERE opp.full_name ILIKE '%Miami Heat%'

-- WRONG: OR join on both home/visitor — creates duplicate rows
JOIN dwh_d_teams opp
  ON g.home_team_id = opp.team_id OR g.visitor_team_id = opp.team_id
WHERE opp.full_name ILIKE '%Miami Heat%'   -- returns player's rows twice
```

---

## RULE 6 — Double Join for Home and Visitor Teams

When a query shows game matchup information (who played whom), join `dwh_d_teams` twice using different aliases.

```sql
SELECT
    g.game_date,
    home_team.full_name AS home_team_name,
    visitor_team.full_name AS visitor_team_name,
    g.home_score,
    g.visitor_score
FROM dwh_d_games g
JOIN dwh_d_teams home_team    ON g.home_team_id    = home_team.team_id
JOIN dwh_d_teams visitor_team ON g.visitor_team_id = visitor_team.team_id;
```

---

## RULE 7 — Plus-Minus Scoped to a Team: Use Player Team Seasons

When a user asks for plus-minus **while playing for a specific team**, scope via `dwh_f_player_team_seasons`. Do not pull career total plus-minus and then filter by team name — the career total includes games for all other teams.

```sql
-- CORRECT: scope games to the specific team via pb.team_id
SELECT
    p.full_name,
    SUM(pb.plus_minus_points) AS total_plus_minus
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_teams t   ON pb.team_id   = t.team_id
WHERE t.full_name ILIKE '%Lakers%'
GROUP BY p.full_name
ORDER BY total_plus_minus DESC;

-- WRONG: career total then filter by team name
SELECT player_id, MAX(plus_minus_points)
FROM dwh_f_player_boxscore
WHERE player_id IN (SELECT player_id FROM dwh_f_player_team_seasons WHERE team_id = 'LAL');
```

---

## RULE 8 — Player Plus-Minus vs Team Plus-Minus Are Different Columns

- **Player plus-minus** → `dwh_f_player_boxscore.plus_minus_points`
- **Team point differential** → derived from `dwh_f_team_boxscore` as `pts_scored - pts_allowed`

Never substitute one for the other.

---

## RULE 9 — Win/Loss Determination: Use Score Columns, Not Plus-Minus

When calculating team win/loss records, use the `home_score` and `visitor_score` columns from `dwh_d_games`. Do not infer wins from plus-minus or any boxscore aggregate.

```sql
-- CORRECT: win/loss from scores
SUM(CASE WHEN g.home_team_id = t.team_id AND g.home_score > g.visitor_score THEN 1
         WHEN g.visitor_team_id = t.team_id AND g.visitor_score > g.home_score THEN 1
         ELSE 0 END) AS wins,
SUM(CASE WHEN g.home_team_id = t.team_id AND g.home_score < g.visitor_score THEN 1
         WHEN g.visitor_team_id = t.team_id AND g.visitor_score < g.home_score THEN 1
         ELSE 0 END) AS losses

-- WRONG: inferring wins from plus-minus
SUM(CASE WHEN pb.plus_minus_points > 0 THEN 1 ELSE 0 END) AS wins
```

---

## RULE 10 — Dimension Joins for Descriptive Output

When a fact table contains only surrogate keys, join the dimension table to provide readable labels. Apply this consistently:

| Fact column | Dimension join | SELECT column |
|---|---|---|
| `pb.player_id` | `JOIN dwh_d_players p ON pb.player_id = p.player_id` | `p.full_name AS player_name` |
| `pb.team_id` | `JOIN dwh_d_teams t ON pb.team_id = t.team_id` | `t.full_name AS team_name` |
| `pb.game_id` | `JOIN dwh_d_games g ON pb.game_id = g.game_id` | `g.game_date` |
| `g.home_team_id` | `JOIN dwh_d_teams ht ON g.home_team_id = ht.team_id` | `ht.full_name AS home_team` |
| `g.visitor_team_id` | `JOIN dwh_d_teams vt ON g.visitor_team_id = vt.team_id` | `vt.full_name AS visitor_team` |

---

## Standard Join Templates

### Player boxscore with all readable labels
```sql
SELECT
    p.full_name AS player_name,
    t.full_name AS team_name,
    g.game_date,
    home_team.full_name AS home_team,
    visitor_team.full_name AS visitor_team,
    pb.points, pb.assists,
    pb.rebounds_offensive + pb.rebounds_defensive AS total_rebounds
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p         ON pb.player_id       = p.player_id
JOIN dwh_d_teams t           ON pb.team_id          = t.team_id
JOIN dwh_d_games g           ON pb.game_id          = g.game_id
JOIN dwh_d_teams home_team   ON g.home_team_id      = home_team.team_id
JOIN dwh_d_teams visitor_team ON g.visitor_team_id  = visitor_team.team_id;
```

### Team-specific player stats (current season)
```sql
WITH target_team AS (
    SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Team Name%'
)
SELECT
    p.full_name AS player_name,
    AVG(pb.points) AS avg_points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_games g   ON pb.game_id   = g.game_id
JOIN target_team t   ON pb.team_id   = t.team_id     -- pb.team_id only
WHERE g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
  AND g.game_type ILIKE '%Regular Season%'
GROUP BY p.full_name
ORDER BY avg_points DESC;
```

---

## RULE 11 — Last N Games: Order by game_date DESC, Never game_id

When the user asks "last 10 games", "last 5 games", always identify qualifying game_ids by ordering `game_date DESC` with LIMIT N. Never use `game_id DESC` — game_id order is not guaranteed to be chronological.

```sql
-- Player's last 5 games averages
WITH recent_games AS (
    SELECT pb.game_id
    FROM dwh_f_player_boxscore pb
    JOIN dwh_d_players p ON pb.player_id = p.player_id
    JOIN dwh_d_games g   ON pb.game_id   = g.game_id
    WHERE p.full_name ILIKE '%LeBron James%'
      AND g.game_type ILIKE '%Regular Season%'
    ORDER BY g.game_date DESC
    LIMIT 5
)
SELECT
    p.full_name,
    AVG(pb.points)  AS avg_points,
    AVG(pb.assists) AS avg_assists,
    AVG(pb.rebounds_offensive + pb.rebounds_defensive) AS avg_rebounds
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN recent_games rg ON pb.game_id   = rg.game_id
GROUP BY p.full_name;

-- Team's last 10 games win/loss record
WITH team AS (
    SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Milwaukee Bucks%'
),
last_10 AS (
    SELECT g.game_id, g.home_team_id, g.visitor_team_id, g.home_score, g.visitor_score
    FROM dwh_d_games g
    JOIN team t ON g.home_team_id = t.team_id OR g.visitor_team_id = t.team_id
    WHERE g.game_type ILIKE '%Regular Season%'
    ORDER BY g.game_date DESC
    LIMIT 10
)
SELECT
    SUM(CASE WHEN (l.home_team_id = t.team_id AND l.home_score > l.visitor_score)
              OR  (l.visitor_team_id = t.team_id AND l.visitor_score > l.home_score)
             THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN (l.home_team_id = t.team_id AND l.home_score < l.visitor_score)
              OR  (l.visitor_team_id = t.team_id AND l.visitor_score < l.home_score)
             THEN 1 ELSE 0 END) AS losses
FROM last_10 l, team t;
```

---

## RULE 12 — Back-to-Back Team Performance: Resolve team_id First, Then LAG

For back-to-back performance queries, resolve team_id from `dwh_d_teams` first. Then use LAG on `game_date` to detect consecutive-day games. Never hardcode team abbreviations in window partition logic.

```sql
WITH celtics AS (
    SELECT team_id FROM dwh_d_teams WHERE full_name ILIKE '%Boston Celtics%'
),
team_games AS (
    SELECT
        g.game_id, g.game_date,
        g.home_team_id, g.visitor_team_id,
        g.home_score, g.visitor_score,
        LAG(g.game_date) OVER (ORDER BY g.game_date) AS prev_game_date
    FROM dwh_d_games g
    JOIN celtics c ON g.home_team_id = c.team_id OR g.visitor_team_id = c.team_id
    WHERE g.game_type ILIKE '%Regular Season%'
      AND g.season_year = (SELECT MAX(season_year) FROM dwh_d_games WHERE game_type ILIKE '%Regular Season%')
),
b2b AS (
    SELECT * FROM team_games
    WHERE game_date - prev_game_date = 1
)
SELECT
    COUNT(*) AS total_b2b_games,
    SUM(CASE WHEN (home_team_id  = (SELECT team_id FROM celtics) AND home_score > visitor_score)
              OR  (visitor_team_id = (SELECT team_id FROM celtics) AND visitor_score > home_score)
             THEN 1 ELSE 0 END) AS b2b_wins,
    SUM(CASE WHEN (home_team_id  = (SELECT team_id FROM celtics) AND home_score < visitor_score)
              OR  (visitor_team_id = (SELECT team_id FROM celtics) AND visitor_score < home_score)
             THEN 1 ELSE 0 END) AS b2b_losses
FROM b2b;
```

---

## Anti-Pattern Summary

| Bad Pattern | Consequence | Fix |
|---|---|---|
| `SELECT pb.team_id` in output | User sees numeric ID | `JOIN dwh_d_teams` → select `t.full_name` |
| `SELECT pb.player_id` in output | User sees numeric ID | `JOIN dwh_d_players` → select `p.full_name` |
| `SELECT pb.game_id` only | No context for user | Add `JOIN dwh_d_games` → select `g.game_date` |
| OR join on game teams for player stats | Includes opponent players | Use `pb.team_id = team_id` |
| Hardcoded abbreviation `'BOS'` in filter | Brittle, may return 0 rows | Look up `team_id` via `dwh_d_teams` |
| Career plus-minus then filter by team | Includes other-team games | Filter `pb.team_id` directly |
| `player plus_minus` from team boxscore | Wrong table | `dwh_f_player_boxscore.plus_minus_points` |
| Win/loss inferred from plus-minus | Wrong logic | Use `home_score > visitor_score` |
| Wrong column: `rebounds_chances_offensive` | Column does not exist | Use `rebounds_offensive` |
| Wrong column: `rebounds_chances_defensive` | Column does not exist | Use `rebounds_defensive` |
| `ORDER BY game_id DESC` for last N games | game_id ≠ chronological order | `ORDER BY game_date DESC` |
| Hardcoded team abbrev in back-to-back LAG | Returns 0 rows if ID mismatch | Resolve team_id from `dwh_d_teams` first |
