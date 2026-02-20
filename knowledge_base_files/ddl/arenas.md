---
name: arenas
description: "Use when the query involves retrieving information about NBA game venues, such as their names, locations, capacities, and opening years. This table is essential for understanding where NBA games are played, analyzing attendance trends, or planning logistics for events. It includes details like the arena's city and state, which are crucial for geographic analysis of games. This table is often used in conjunction with team or game data to provide context about the venue."
tags: [arenas, venues, locations, nba]
priority: medium
---

# DDL

```sql
CREATE TABLE arenas (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(200) NOT NULL,      -- e.g., 'Crypto.com Arena'
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100) DEFAULT 'USA',
    capacity        INT,
    opened_year     INT,
    created_at      TIMESTAMP DEFAULT NOW()
);
```

## Column Semantics

- **id**: A unique identifier for each arena, used as a primary key. This is crucial for joining with other tables, such as game schedules or team home venues.
- **name**: The official name of the arena, such as 'Madison Square Garden'. This is often used in SELECT statements to display venue names in reports or dashboards.
- **city**: The city where the arena is located. Useful for geographic filtering or grouping, e.g., finding all arenas in Los Angeles.
- **state**: The state where the arena is located. This can be used similarly to the city for regional analysis.
- **country**: The country of the arena, defaulting to 'USA'. This is relevant for international games or when expanding the database to include non-U.S. venues.
- **capacity**: The maximum number of spectators the arena can accommodate. This is often used in analysis of attendance data or to assess the size of the venue.
- **opened_year**: The year the arena was opened. This can be used to analyze the age of venues or trends in arena construction.
- **created_at**: Timestamp of when the record was created. Typically used for auditing or tracking changes over time.

## Common Query Patterns

- Retrieve all arenas in a specific city or state: `SELECT * FROM arenas WHERE city = 'Los Angeles';`
- Find arenas with a capacity greater than a certain number: `SELECT name FROM arenas WHERE capacity > 20000;`
- List arenas opened before a certain year: `SELECT name, opened_year FROM arenas WHERE opened_year < 2000;`

## Join Relationships

- Typically joined with a `games` table on the arena's `id` to find out which games were played at a specific venue.
- Can be joined with a `teams` table to determine which team calls an arena their home venue.
- May be used in conjunction with a `tickets` table to analyze sales data by venue.