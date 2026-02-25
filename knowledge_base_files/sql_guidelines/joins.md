---
name: joins
description: "Use when the query involves linking tables across the schema to retrieve related data. Covers player-team joins, game-team joins, player plus-minus while on a specific team, always resolving team names via joins (never exposing raw IDs), and double-join scenarios."
tags: [joins, foreign keys, multi-table queries, data integrity, team names, plus-minus by team]
priority: high
---

## Standard Join Patterns

### Joining Players with Teams
```sql
SELECT p.full_name, p.position, t.full_name AS team_name, t.city
FROM dwh_d_players p
JOIN dwh_d_teams t ON p.team_id = t.team_id;
```

### Joining Games with Teams (Double Join)
```sql
SELECT
    g.game_id, g.game_date,
    home_team.full_name AS home_team_name,
    visitor_team.full_name AS visitor_team_name
FROM dwh_d_games g
JOIN dwh_d_teams home_team    ON g.home_team_id    = home_team.team_id
JOIN dwh_d_teams visitor_team ON g.visitor_team_id = visitor_team.team_id;
```
---

## Always Resolve Team Names — Never Expose Raw IDs

User-facing output must **always show team names**, not raw `team_id` or `game_id` values. Join `dwh_d_teams` whenever a team identifier appears in a result.

```sql
-- CORRECT: user sees "Golden State Warriors"
SELECT p.full_name, t.full_name AS team_name, pb.points
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_d_teams   t ON pb.team_id   = t.team_id;

-- WRONG: user sees "GSW" or a numeric ID
SELECT p.full_name, pb.team_id, pb.points FROM ...;
```
Apply the same rule for opponent team in game queries — always join `dwh_d_teams` twice (as `home_team` and `visitor_team`) to show readable names.

---

## Plus-Minus Filtered by Team — Join Season Table

When a user asks for plus-minus **while playing for a specific team**, join `dwh_f_player_team_seasons` to scope games correctly. Do **not** use the player's career total and then filter by team name.

```sql
-- CORRECT: sums plus-minus only in games played for the Lakers
SELECT
    p.full_name,
    SUM(pb.plus_minus_points) AS total_plus_minus,
    AVG(pb.plus_minus_points) AS avg_plus_minus
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p ON pb.player_id = p.player_id
JOIN dwh_f_player_team_seasons pts
     ON pb.player_id = pts.player_id
    AND pb.game_id IN (
        SELECT game_id FROM dwh_d_games
        WHERE season_year = pts.season_year
    )
WHERE pts.team_id = 'LAL'
GROUP BY p.full_name
ORDER BY total_plus_minus DESC;

-- WRONG: finds player with highest career plus-minus who has EVER played for Lakers
SELECT player_id, MAX(plus_minus_points)
FROM dwh_f_player_boxscore
WHERE player_id IN (SELECT player_id FROM dwh_f_player_team_seasons WHERE team_id = 'LAL');
```
---

## Player's Own Plus-Minus vs Team Plus-Minus

**Player plus-minus** comes from `dwh_f_player_boxscore.plus_minus_points`.  
**Team plus-minus** (point differential) comes from `dwh_f_team_boxscore`.  
Do not substitute one for the other.

```sql
-- Player plus-minus
SELECT player_id, AVG(plus_minus_points) FROM dwh_f_player_boxscore GROUP BY player_id;

-- WRONG: team differential ≠ player plus-minus
SELECT team_id, SUM(pts_scored - pts_allowed) FROM dwh_f_team_boxscore GROUP BY team_id;
```
---

## Multi-Table Query Example
```sql
SELECT
    pb.player_id, p.full_name, g.game_date,
    ht.full_name AS home_team_name,
    vt.full_name AS visitor_team_name,
    pb.points,
    pb.assists,
    pb.rebounds_offensive + pb.rebounds_defensive AS total_rebounds
FROM dwh_f_player_boxscore pb
JOIN dwh_d_players p  ON pb.player_id = p.player_id
JOIN dwh_d_games g    ON pb.game_id   = g.game_id
JOIN dwh_d_teams ht   ON g.home_team_id    = ht.team_id
JOIN dwh_d_teams vt   ON g.visitor_team_id = vt.team_id
WHERE g.season_year = '2024';
```
---

## Gotchas and Anti-Patterns

- **Incorrect join conditions** lead to Cartesian products or missing rows — verify FK relationships.
- **Missing aliases** when joining the same table multiple times causes column ambiguity.
- **Data duplication** in one-to-many joins — use `COUNT(DISTINCT game_id)` where appropriate.
- **Exposing IDs in output** — always join lookup tables to replace IDs with readable names.
- **Column name errors** — always verify exact column names against the schema (e.g., `rebounds_offensive`, NOT `rebounds_chances_offensive`).