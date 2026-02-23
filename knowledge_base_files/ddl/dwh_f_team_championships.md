---
name: dwh_f_team_championships
description: "Use when the query involves identifying NBA teams that have won championships, the years they won, and their opponents in the finals. This table is essential for analyzing team success over time, comparing championship wins, and understanding historical matchups between teams. Ideal for queries about team performance, championship history, and rivalries in the NBA."
tags: [championships, teams, NBA history]
priority: high
---

# DDL

```sql
CREATE TABLE dwh_f_team_championships (team_id text, yearawarded text, oppositeteam text);
```

## Column Semantics

- **team_id**: Represents the unique identifier for an NBA team. This is crucial for linking to other tables containing team-specific data. Typically used in SELECT and JOIN clauses. Example values include 'LAL' for Los Angeles Lakers or 'BOS' for Boston Celtics.

- **yearawarded**: Indicates the year in which the team won the championship. This is a text field but represents a year, such as '2020' or '1996'. Commonly used in WHERE clauses to filter results by specific years or ranges.

- **oppositeteam**: The name of the team that was defeated in the finals. This provides context for the championship win, highlighting historical rivalries and matchups. Example values might include 'Miami Heat' or 'Golden State Warriors'. Used in SELECT statements to display or analyze opponent data.

## Common Query Patterns

- Retrieve all championship wins for a specific team:
  ```sql
  SELECT yearawarded FROM dwh_f_team_championships WHERE team_id = 'LAL';
  ```

- Compare championship wins between two teams over a period:
  ```sql
  SELECT team_id, COUNT(*) as championships FROM dwh_f_team_championships WHERE yearawarded BETWEEN '2000' AND '2020' GROUP BY team_id;
  ```

- List all teams that defeated a specific team in the finals:
  ```sql
  SELECT team_id FROM dwh_f_team_championships WHERE oppositeteam = 'Boston Celtics';
  ```

## Join Relationships

- **team_id**: This column can be joined with a team dimension table (e.g., `dwh_dim_teams`) on `team_id` to retrieve additional team details such as team name, location, and conference.
- **yearawarded**: Can be used to join with a calendar or season table to provide more context about the season, such as regular season records or playoff performance.
- **oppositeteam**: While not a direct foreign key, this can be used in conjunction with a team dimension table to gather more information about the opposing teams in championship matchups.