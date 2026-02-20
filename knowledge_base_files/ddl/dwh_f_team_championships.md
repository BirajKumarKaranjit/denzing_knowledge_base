---
name: dwh_f_team_championships
description: "Use when the query involves identifying NBA teams that have won championships, the years they won, and the opposing teams they defeated in the finals. This table is essential for analyzing team success over time, comparing championship wins across different franchises, and understanding historical matchups in the NBA Finals. It is particularly useful for queries related to team performance, historical achievements, and rivalry analysis."
tags: [championships, nba finals, team success]
priority: medium
---

# DDL

```sql
CREATE TABLE dwh_f_team_championships (team_id text, yearawarded text, oppositeteam text);
```

## Column Semantics

- **team_id**: Represents the unique identifier for an NBA team. This is typically a text code that corresponds to a specific franchise. It is crucial for identifying which team won the championship in a given year. Commonly used in SELECT and WHERE clauses to filter results by team.
  
- **yearawarded**: Indicates the year in which the team was awarded the championship. This is a text field, often formatted as a four-digit year (e.g., "2023"). It is used to track the timeline of championships and is frequently used in WHERE clauses to filter by specific years or ranges.

- **oppositeteam**: Denotes the team that the championship-winning team defeated in the NBA Finals. This is a text field containing the name or identifier of the opposing team. It provides context for the championship win and is often used in SELECT statements to display matchups or in WHERE clauses to filter by specific opponents.

## Common Query Patterns

- Retrieve all championship wins for a specific team:
  ```sql
  SELECT yearawarded FROM dwh_f_team_championships WHERE team_id = 'LAL';
  ```

- List all teams that won championships in a specific decade:
  ```sql
  SELECT team_id, yearawarded FROM dwh_f_team_championships WHERE yearawarded BETWEEN '2010' AND '2019';
  ```

- Compare championship wins between two teams:
  ```sql
  SELECT team_id, COUNT(*) as championships FROM dwh_f_team_championships WHERE team_id IN ('BOS', 'LAL') GROUP BY team_id;
  ```

## Join Relationships

- **team_id**: This column can be joined with a team dimension table (e.g., `dwh_d_teams`) on the `team_id` to retrieve additional team details such as team name, location, and historical data.
- **oppositeteam**: This column can also be joined with the same team dimension table to get details about the opposing teams in the finals.
- This table is often used in conjunction with player statistics tables to analyze player contributions during championship seasons.