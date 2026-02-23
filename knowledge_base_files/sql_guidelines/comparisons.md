---
name: comparisons
description: "Use when the query compares players against each other, compares a player to a league average, compares two teams head-to-head, or ranks entities relative to a threshold. Covers CASE WHEN expressions, subquery comparisons, HAVING-based filters, and conditional aggregations. Choose this for queries like 'which players scored above the league average', 'compare player A vs player B', or 'teams that scored more than X points per game'."
tags: [comparisons, case-when, subquery, league-average, head-to-head, ranking]
priority: medium
---

# Comparison Patterns for NBA Analytics

## Player vs League Average

### Above-average scorers in a season
```sql
WITH league_avg AS (
    SELECT AVG(bs.points) AS avg_points
    FROM dwh_f_player_boxscore bs
    JOIN dwh_d_games g ON bs.game_id = g.game_id
    WHERE g.season_year = '2022' AND g.game_type = 'regular'
)
SELECT
    p.full_name,
    ROUND(AVG(bs.points), 1) AS ppg
FROM dwh_f_player_boxscore bs
JOIN dwh_d_players p ON bs.player_id = p.player_id
JOIN dwh_d_games g    ON bs.game_id = g.game_id
CROSS JOIN league_avg
WHERE g.season_year = '2022' AND g.game_type = 'regular'
GROUP BY p.player_id, p.full_name, league_avg.avg_points
HAVING AVG(bs.points) > league_avg.avg_points
ORDER BY ppg DESC;
```

## Player vs Player (head-to-head season stats)
```sql
SELECT
    p.full_name,
    ROUND(SUM(bs.points) / NULLIF(COUNT(DISTINCT bs.game_id), 0), 1) AS ppg,
    ROUND(SUM(bs.assists) / NULLIF(COUNT(DISTINCT bs.game_id), 0), 1) AS apg
FROM dwh_f_player_boxscore bs
JOIN dwh_d_players p ON bs.player_id = p.player_id
JOIN dwh_d_games g   ON bs.game_id   = g.game_id
WHERE g.season_year = '2022'
  AND g.game_type   = 'regular'
  AND p.full_name IN ('LeBron James', 'Kevin Durant')
GROUP BY p.player_id, p.full_name
ORDER BY ppg DESC;
```

## CASE WHEN for Conditional Categories
```sql
SELECT
    p.full_name,
    AVG(bs.points) AS avg_points,
    CASE
        WHEN AVG(bs.points) >= 25 THEN 'Elite Scorer'
        WHEN AVG(bs.points) >= 15 THEN 'Starter'
        ELSE 'Role Player'
    END AS scorer_tier
FROM dwh_f_player_boxscore bs
JOIN dwh_d_players p ON bs.player_id = p.player_id
JOIN dwh_d_games g   ON bs.game_id   = g.game_id
WHERE g.season_year = '2022' AND g.game_type = 'regular'
GROUP BY p.player_id, p.full_name;
```

## Team Head-to-Head
```sql
SELECT
    ht.full_name AS home_team,
    at.full_name AS away_team,
    g.home_score,
    g.visitor_score,
    CASE WHEN g.home_score > g.visitor_score THEN ht.full_name ELSE at.full_name END AS winner
FROM dwh_d_games g
JOIN dwh_d_teams ht ON g.home_team_id    = ht.team_id
JOIN dwh_d_teams at ON g.visitor_team_id = at.team_id
WHERE (ht.abbreviation = 'LAL' AND at.abbreviation = 'GSW')
   OR (ht.abbreviation = 'GSW' AND at.abbreviation = 'LAL')
ORDER BY g.game_date;
```

## Threshold Comparisons with HAVING

```sql
-- Teams averaging over 115 points per game
HAVING ROUND(AVG(tbs.points), 1) > 115

-- Players with at least 20 games (statistical minimum)
HAVING COUNT(DISTINCT bs.game_id) >= 20
```

