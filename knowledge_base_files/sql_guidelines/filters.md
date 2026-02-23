---
name: filters
description: "Use when the query filters data by player name, team name, season, game type (regular season vs playoffs), position, award type, draft class, or active status. Covers WHERE clause patterns, IN/NOT IN lists, LIKE/ILIKE for partial name matches, and filtering by game type or season. Choose this for any query that narrows results to a specific player, team, time period, or category."
tags: [filters, where-clause, season, game-type, player-lookup, team-lookup]
priority: high
---

# Filter Patterns for NBA Analytics

## Player Filtering

### By exact player_id (preferred — always use ID for joins/filters)
```sql
WHERE bs.player_id = '2544'   -- LeBron James NBA player ID
```

### By full_name when player_id is unknown (case-insensitive partial match)
```sql
WHERE LOWER(p.full_name) LIKE '%lebron%'
```

### By active status
```sql
WHERE p.roster_status = 'Active'
-- or
WHERE p.to_year = '2024'   -- still playing in 2024-25 season
```

## Season Filtering

```sql
-- Single season
WHERE g.season_year = '2022'   -- means the 2022-23 NBA season

-- Range of seasons
WHERE g.season_year BETWEEN '2020' AND '2023'
```

> **Convention**: `season_year` stores the year the season STARTS.
> '2022' = the 2022-23 season. Always clarify with the user if ambiguous.

## Game Type Filtering

```sql
-- Regular season only (default when not specified by user)
WHERE g.game_type = 'regular'

-- Playoffs only
WHERE g.game_type = 'playoff'

-- All games (remove the game_type filter entirely)
```

## Team Filtering

```sql
-- By team abbreviation (e.g., LAL = Los Angeles Lakers)
WHERE t.abbreviation = 'LAL'

-- By full name (case-insensitive)
WHERE LOWER(t.full_name) LIKE '%lakers%'

-- By conference/division
WHERE t.conference = 'West'
WHERE t.division   = 'Pacific'
```

## Award Filtering

```sql
-- Filter award type
WHERE a.description ILIKE '%MVP%'
WHERE a.description ILIKE '%All-NBA%'

-- Season of award
WHERE a.season = '2022-23'
```

## Position Filtering

```sql
WHERE p.position = 'Guard'
WHERE p.position IN ('Point Guard', 'Shooting Guard')
```

## Draft Filtering

```sql
WHERE p.draft_year = '2003'   -- 2003 draft class (LeBron, Carmelo, etc.)
WHERE p.draft_round = '1'
```

## Combining Filters Safely

Always place the most selective filter first in the WHERE clause:
```sql
WHERE g.season_year = '2022'   -- most selective (reduces rows first)
  AND g.game_type = 'regular'
  AND bs.player_id = '2544'
```

