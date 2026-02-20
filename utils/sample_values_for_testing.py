
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
    "players": """
CREATE TABLE players (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name       VARCHAR(200) NOT NULL,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    date_of_birth   DATE,
    nationality     VARCHAR(100),
    position        VARCHAR(10),        -- PG, SG, SF, PF, C, G, F, G-F, F-C
    height_cm       DECIMAL(5,2),
    weight_kg       DECIMAL(5,2),
    jersey_number   INT,
    is_active       BOOLEAN DEFAULT TRUE,
    draft_year      INT,
    draft_round     INT,
    draft_pick      INT,
    team_id         UUID REFERENCES teams(id),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);
""",

    "teams": """
CREATE TABLE teams (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(200) NOT NULL,      -- e.g., 'Los Angeles Lakers'
    abbreviation    VARCHAR(5) NOT NULL,         -- e.g., 'LAL'
    city            VARCHAR(100),
    conference      VARCHAR(10),                 -- 'East' or 'West'
    division        VARCHAR(50),                 -- e.g., 'Pacific', 'Atlantic'
    arena_id        UUID REFERENCES arenas(id),
    head_coach      VARCHAR(200),
    founded_year    INT,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);
""",

    "games": """
CREATE TABLE games (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    game_date       DATE NOT NULL,
    season_year     INT NOT NULL,               -- e.g., 2023 means 2023-24 season
    game_type       VARCHAR(20) NOT NULL,       -- 'regular', 'playoff', 'preseason'
    home_team_id    UUID NOT NULL REFERENCES teams(id),
    away_team_id    UUID NOT NULL REFERENCES teams(id),
    home_score      INT,
    away_score      INT,
    winner_team_id  UUID REFERENCES teams(id),
    overtime        BOOLEAN DEFAULT FALSE,
    attendance      INT,
    arena_id        UUID REFERENCES arenas(id),
    created_at      TIMESTAMP DEFAULT NOW()
);
""",

    "box_scores": """
CREATE TABLE box_scores (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    game_id                 UUID NOT NULL REFERENCES games(id),
    player_id               UUID NOT NULL REFERENCES players(id),
    team_id                 UUID NOT NULL REFERENCES teams(id),
    minutes_played          DECIMAL(5,2),
    points                  INT DEFAULT 0,
    field_goals_made        INT DEFAULT 0,
    field_goal_attempts     INT DEFAULT 0,
    three_pointers_made     INT DEFAULT 0,
    three_point_attempts    INT DEFAULT 0,
    free_throws_made        INT DEFAULT 0,
    free_throw_attempts     INT DEFAULT 0,
    offensive_rebounds      INT DEFAULT 0,
    defensive_rebounds      INT DEFAULT 0,
    total_rebounds          INT DEFAULT 0,
    assists                 INT DEFAULT 0,
    steals                  INT DEFAULT 0,
    blocks                  INT DEFAULT 0,
    turnovers               INT DEFAULT 0,
    personal_fouls          INT DEFAULT 0,
    plus_minus              INT,
    starter                 BOOLEAN DEFAULT FALSE,
    created_at              TIMESTAMP DEFAULT NOW(),
    UNIQUE (game_id, player_id)
);
""",

    "seasons": """
CREATE TABLE seasons (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_year     INT NOT NULL UNIQUE,        -- e.g., 2023 for the 2023-24 season
    start_date      DATE NOT NULL,
    end_date        DATE NOT NULL,
    playoff_start   DATE,
    champion_team_id UUID REFERENCES teams(id),
    mvp_player_id   UUID REFERENCES players(id),
    created_at      TIMESTAMP DEFAULT NOW()
);
""",

    "player_awards": """
CREATE TABLE player_awards (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id       UUID NOT NULL REFERENCES players(id),
    season_year     INT NOT NULL,
    award_type      VARCHAR(100) NOT NULL,  -- 'MVP', 'DPOY', 'ROY', 'SMOY', 'All-Star', 'All-NBA First Team', etc.
    team_at_time    UUID REFERENCES teams(id),
    created_at      TIMESTAMP DEFAULT NOW()
);
""",

    "arenas": """
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
""",

    "salaries": """
CREATE TABLE salaries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id       UUID NOT NULL REFERENCES players(id),
    team_id         UUID NOT NULL REFERENCES teams(id),
    season_year     INT NOT NULL,
    salary_usd      BIGINT NOT NULL,            -- Annual salary in USD (not millions)
    contract_type   VARCHAR(50),               -- 'max', 'rookie', 'veteran_min', 'mid_level'
    guaranteed      BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (player_id, season_year)
);
""",
}