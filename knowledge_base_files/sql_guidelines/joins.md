---
name: joins
description: "Use when the query involves linking tables across the schema to retrieve related data. This includes scenarios where you need to combine player statistics with game details, or when you need to aggregate team performance across seasons. Be cautious of double-join scenarios, such as self-referencing tables or joining the same table twice, which can lead to incorrect results if not handled properly. Ensure that joins are based on correct foreign key relationships to maintain data integrity and accuracy."
tags: [joins, foreign keys, multi-table queries, data integrity]
priority: high
---

## Standard Join Patterns

When joining tables, it's crucial to use the correct columns that define the relationships between them. Here are some common join patterns using the provided schema:

### Joining Players with Teams

To retrieve player details along with their team information, join `dwh_d_players` with `dwh_d_teams` using the `team_id`:

```sql
SELECT 
    p.full_name,
    p.position,
    t.full_name AS team_name,
    t.city
FROM 
    dwh_d_players p
JOIN 
    dwh_d_teams t ON p.team_id = t.team_id;
```

### Joining Games with Teams

To get details of games along with the home and visitor team names, join `dwh_d_games` with `dwh_d_teams` twice:

```sql
SELECT 
    g.game_id,
    g.game_date,
    home_team.full_name AS home_team_name,
    visitor_team.full_name AS visitor_team_name
FROM 
    dwh_d_games g
JOIN 
    dwh_d_teams home_team ON g.home_team_id = home_team.team_id
JOIN 
    dwh_d_teams visitor_team ON g.visitor_team_id = visitor_team.team_id;
```

## Double-Join Gotchas

### Self-Referencing Tables

Be cautious when joining a table to itself. Ensure that aliases are used to differentiate between the instances of the table:

```sql
SELECT 
    p1.full_name AS player_name,
    p2.full_name AS teammate_name
FROM 
    dwh_d_players p1
JOIN 
    dwh_d_players p2 ON p1.team_id = p2.team_id AND p1.player_id <> p2.player_id;
```

### Joining the Same Table Twice

When joining the same table twice, such as `dwh_d_teams` in the games example above, ensure that each join has a distinct alias to avoid confusion and errors.

## Multi-Table Query Example

Here's a complete example that combines player statistics with game and team details:

```sql
SELECT 
    pb.player_id,
    p.full_name,
    g.game_date,
    ht.full_name AS home_team_name,
    vt.full_name AS visitor_team_name,
    pb.points,
    pb.assists,
    pb.rebounds_offensive + pb.rebounds_defensive AS total_rebounds
FROM 
    dwh_f_player_boxscore pb
JOIN 
    dwh_d_players p ON pb.player_id = p.player_id
JOIN 
    dwh_d_games g ON pb.game_id = g.game_id
JOIN 
    dwh_d_teams ht ON g.home_team_id = ht.team_id
JOIN 
    dwh_d_teams vt ON g.visitor_team_id = vt.team_id
WHERE 
    g.season_year = '2023';
```

## Gotchas and Anti-Patterns

- **Incorrect Join Conditions**: Always verify that the join conditions match the intended relationships. Incorrect joins can lead to Cartesian products or missing data.
- **Missing Aliases**: When joining the same table multiple times, use aliases to clearly distinguish between different instances.
- **Data Duplication**: Be aware of potential data duplication when joining tables with one-to-many relationships. Use aggregation functions if needed to summarize data correctly.