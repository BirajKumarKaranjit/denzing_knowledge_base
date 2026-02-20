
sql_guidelines_content = """---
        name: sql_guidelines
        description: "Query patterns, join conventions, and SQL best practices for the NBA analytics database."
        ---

        # SQL Guidelines

        ## General Conventions
        - Always use table aliases for readability (e.g., p for players, g for games)
        - Use DATE_TRUNC('season', game_date) for season-level aggregations
        - Exclude preseason games unless explicitly requested (filter by game_type = 'regular')

        ## Common Join Patterns
        - box_scores JOIN players ON box_scores.player_id = players.id
        - box_scores JOIN games ON box_scores.game_id = games.id
        - games JOIN teams ON games.home_team_id = teams.id OR games.away_team_id = teams.id

        ## Aggregation Rules
        - Per-game stats: always divide by COUNT(DISTINCT game_id)
        - Season totals: GROUP BY player_id, season_year
        - Use NULLIF(denominator, 0) for all division operations

        ## Date Handling
        - game_date is TIMESTAMP — use DATE_TRUNC or CAST to DATE for day-level queries
        - Season year convention: 2023 means the 2023-24 season
        - Never use CURRENT_DATE — filter dynamically on MAX(game_date) in the data

        ## Performance
        - Filter by season_year before joining (reduces scan size significantly)
        - Use player_id / team_id for joins, never player_name (non-indexed)
        """


biz_rules_content = """---
        name: business_rules
        description: "NBA domain metric definitions, KPI formulas, and basketball statistics calculations."
        ---

        # Business Rules & Metric Definitions

        ## Core Counting Stats
        - **Points (PTS)**: Direct from box_scores.points
        - **Rebounds (REB)**: offensive_rebounds + defensive_rebounds
        - **Assists (AST)**: Direct from box_scores.assists

        ## Advanced Metrics
        - **True Shooting % (TS%)**: points / (2 * (field_goal_attempts + 0.44 * free_throw_attempts))
        - **Effective FG% (eFG%)**: (field_goals_made + 0.5 * three_pointers_made) / field_goal_attempts
        - **Player Efficiency Rating (PER)**: Use the standard Hollinger formula (complex — prefer pre-computed columns if available)

        ## Game Filters
        - Regular season only: WHERE game_type = 'regular'
        - Playoffs only: WHERE game_type = 'playoff'
        - Default to regular season unless user specifies playoffs

        ## Player Activity
        - Active players: WHERE is_active = TRUE in players table
        - Minimum games filter for statistical relevance: typically >= 20 games per season
        """


response_guidelines_content = """---
            name: response_guidelines
            description: "Output formatting and response quality guidelines for NBA analytics SQL generation."
            ---

            # Response Guidelines

            ## SQL Output
            - Return only executable SQL in a ```sql ... ``` block
            - Include a brief SQL comment explaining the approach for complex queries

            ## Numeric Formatting
            - Round percentages to 2 decimal places
            - Round counting stats to 1 decimal place for per-game averages
            - Integer values (rank, game count) should not be rounded

            ## Column Aliasing
            - Use descriptive aliases: points_per_game not ppg (unless standard NBA abbreviation)
            - Season year should be aliased as season_year not just year
            """

Sample_NBA_DDL_DICT: dict[str, str] = {
    'dwh_d_games': 'CREATE TABLE dwh_d_games (game_id text, gamecode text, season_year text, season text, game_date date, game_time time without time zone, arena_name text, home_team_id text, visitor_team_id text, home_score numeric, visitor_score numeric, game_type text, playoff_round text);',
    'dwh_d_players': 'CREATE TABLE dwh_d_players (player_id text, team_id text, full_name text, player_slug text, birthdate date, school text, country text, last_affiliation text, player_height numeric, player_weight numeric, season_experience numeric, jersey_number text, position text, roster_status text, from_year text, to_year text, draft_year text, draft_round text, draft_number text, greatest_75_flag text);',
    'dwh_d_player_nicknames': 'CREATE TABLE dwh_d_player_nicknames (player_id text, nickname text, description text);',
    'dwh_d_teams': 'CREATE TABLE dwh_d_teams (team_id text, full_name text, abbreviation text, nickname text, city text, state text, year_founded text, conference text, division text, active_status text);',
    'dwh_f_player_awards': 'CREATE TABLE dwh_f_player_awards (player_id text, team_name text, description text, all_nba_team_number text, season text, award_month date, award_week date, conference text);',
    'dwh_f_player_boxscore': 'CREATE TABLE dwh_f_player_boxscore (id text, game_id text, team_id text, player_id text, position text, comment text, jerseynum text, minutes numeric, field_goals_made numeric, field_goals_attempted numeric, three_pointers_made numeric, three_pointers_attempted numeric, free_throws_made numeric, free_throws_attempted numeric, rebounds_offensive numeric, rebounds_defensive numeric, assists numeric, steals numeric, blocks numeric, turnovers numeric, fouls_personal numeric, points numeric, plus_minus_points numeric, percentage_field_goals_attempted_2pt numeric, percentage_field_goals_attempted_3pt numeric, percentage_points_2pt numeric, percentage_points_midrange_2pt numeric, percentage_points_3pt numeric, percentage_points_fastbreak numeric, percentage_points_free_throw numeric, percentage_points_off_turnovers numeric, percentage_points_paint numeric, percentage_assisted_2pt numeric, percentage_unassisted_2pt numeric, percentage_assisted_3pt numeric, percentage_unassisted_3pt numeric, percentage_assisted_fgm numeric, percentage_unassisted_fgm numeric, free_throw_attempt_rate numeric, offensive_rebound_percentage numeric, estimated_offensive_rating numeric, offensive_rating numeric, estimated_defensive_rating numeric, defensive_rating numeric, assist_percentage numeric, assist_ratio numeric, defensive_rebound_percentage numeric, rebound_percentage numeric, turnover_ratio numeric, usage_percentage numeric, estimated_usage_percentage numeric, pie numeric, technical_foul_count numeric, technical_foul_description1 text, technical_foul_description2 text);',
    'dwh_f_player_team_seasons': 'CREATE TABLE dwh_f_player_team_seasons (player_id text, season text, team_id text, game_type text, games_played numeric);',
    'dwh_f_player_tracking': 'CREATE TABLE dwh_f_player_tracking (id text, game_id text, team_id text, player_id text, position text, speed numeric, distance numeric, rebound_chances_offensive numeric, rebound_chances_defensive numeric, touches numeric, secondary_assists numeric, free_throw_assists numeric, passes numeric, contested_field_goals_made numeric, contested_field_goals_attempted numeric, uncontested_field_goals_made numeric, uncontested_field_goals_attempted numeric, defended_at_rim_field_goals_made numeric, defended_at_rim_field_goals_attempted numeric);',
    'dwh_f_team_boxscore': 'CREATE TABLE dwh_f_team_boxscore (id text, game_id text, team_id text, minutes numeric, field_goals_made numeric, field_goals_attempted numeric, three_pointers_made numeric, three_pointers_attempted numeric, free_throws_made numeric, free_throws_attempted numeric, rebounds_offensive numeric, rebounds_defensive numeric, assists numeric, steals numeric, blocks numeric, turnovers numeric, fouls_personal numeric, points numeric, plus_minus_points numeric, percentage_points_midrange_2pt numeric, percentage_points_fastbreak numeric, percentage_points_off_turnovers numeric, percentage_points_paint numeric, percentage_assisted_2pt numeric, percentage_unassisted_2pt numeric, percentage_assisted_3pt numeric, percentage_unassisted_3pt numeric, percentage_assisted_fgm numeric, percentage_unassisted_fgm numeric, team_turnover_percentage numeric, offensive_rebound_percentage numeric, opp_team_turnover_percentage numeric, opp_offensive_rebound_percentage numeric, estimated_offensive_rating numeric, estimated_defensive_rating numeric, defensive_rating numeric, estimated_net_rating numeric, assist_ratio numeric, defensive_rebound_percentage numeric, rebound_percentage numeric, turnover_ratio numeric, usage_percentage numeric, estimated_usage_percentage numeric, estimated_pace numeric, pace numeric, possessions numeric, pie numeric, qtr1_points numeric, qtr2_points numeric, qtr3_points numeric, qtr4_points numeric, ot1_points numeric, ot2_points numeric, ot3_points numeric, ot4_points numeric, ot5_points numeric, ot6_points numeric, ot7_points numeric, ot8_points numeric, ot9_points numeric, ot10_points numeric);',
    'dwh_f_team_championships': 'CREATE TABLE dwh_f_team_championships (team_id text, yearawarded text, oppositeteam text);'}
