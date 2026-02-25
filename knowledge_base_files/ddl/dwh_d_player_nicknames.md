---
name: dwh_d_player_nicknames
description: "Use when the query involves retrieving or analyzing player nicknames and their descriptions within the analytics database. This table is essential for understanding the aliases or alternate names that players might use, which can be crucial for personalized marketing, user engagement analysis, or behavioral studies. It helps in linking player identities across different datasets where nicknames are used instead of formal identifiers. Queries often involve filtering by specific nicknames or descriptions to segment players based on their chosen identifiers."
tags: [player, nickname, identity]
priority: medium
---

# DDL

```sql
CREATE TABLE dwh_d_player_nicknames (player_id text, nickname text, description text);
```

## Column Semantics

- **player_id**: Represents the unique identifier for each player. This is a text field that typically corresponds to a primary key in a player dimension table. It is crucial for joining with other tables to gather more comprehensive player data. Commonly used in WHERE clauses to filter data for specific players or in JOIN conditions.
  
- **nickname**: Contains the nickname or alias of the player. This text field can include a wide variety of values, often reflecting personal or in-game identities chosen by players. It is frequently used in SELECT statements to display player nicknames or in WHERE clauses to filter players by their nicknames.

- **description**: Provides additional context or information about the nickname. This could include the origin of the nickname or any special notes. It is a text field that might be used in SELECT statements to provide more detailed information about a player's nickname, although it is less commonly used in filtering or grouping operations.

## Common Query Patterns

- Retrieve all nicknames for a specific player: `SELECT nickname FROM dwh_d_player_nicknames WHERE player_id = '12345';`
- Find players with a specific nickname: `SELECT player_id FROM dwh_d_player_nicknames WHERE nickname = 'Ace';`
- List all nicknames and descriptions for analysis: `SELECT nickname, description FROM dwh_d_player_nicknames;`
- Filter players based on a keyword in the description: `SELECT player_id FROM dwh_d_player_nicknames WHERE description LIKE '%champion%';`

## Join Relationships

- Typically joined with a player dimension table using `player_id` to enrich player data with nickname information. For example, `JOIN dwh_d_players ON dwh_d_player_nicknames.player_id = dwh_d_players.player_id` to combine nickname data with other player attributes.