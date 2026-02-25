---
name: dwh_f_team_championships
description: "Use when the query involves analyzing team championship victories, historical performance, or competitive matchups. This table provides insights into which teams have won championships, the years they were awarded, and the opposing teams they faced. It is essential for understanding team success over time and comparing performance against specific competitors. Ideal for queries focusing on championship trends, rivalry analysis, and historical sports data analytics."
tags: [championships, teams, sports, analytics]
priority: high
---

# DDL

```sql
CREATE TABLE dwh_f_team_championships (team_id text, yearawarded text, oppositeteam text);
```

## Column Semantics

- **team_id**: Represents the unique identifier for a team that has won a championship. Typically a text string, it is used to filter results for specific teams. Commonly used in WHERE and JOIN clauses to connect with other team-related tables.
  
- **yearawarded**: Indicates the year in which the championship was awarded to the team. Stored as text, but represents a year (e.g., '2020', '2021'). This column is crucial for temporal analysis and is often used in WHERE clauses to filter by specific years or in GROUP BY clauses to aggregate data by year.
  
- **oppositeteam**: Denotes the team that was the opponent in the championship match. Like team_id, it is a text string and is used to analyze matchups and rivalries. This column is useful in SELECT statements to display opponent information and in WHERE clauses to filter by specific opponents.

## Common Query Patterns

- Retrieve all championships won by a specific team:
  ```sql
  SELECT * FROM dwh_f_team_championships WHERE team_id = 'TeamA';
  ```

- List all championships in a particular year:
  ```sql
  SELECT * FROM dwh_f_team_championships WHERE yearawarded = '2021';
  ```

- Find all opponents a team has faced in championship matches:
  ```sql
  SELECT oppositeteam FROM dwh_f_team_championships WHERE team_id = 'TeamA';
  ```

## Join Relationships

- This table can be joined with a team dimension table using `team_id` to enrich data with team details such as team name, location, or league.
- Potential joins with a calendar table using `yearawarded` to integrate additional temporal data like season start and end dates.
- Join with a match results table using `team_id` and `oppositeteam` to analyze detailed match statistics and outcomes.