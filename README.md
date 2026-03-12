# Knowledge Base System for Text2SQL

A semantic retrieval layer that injects only **relevant** table schemas and
SQL guidelines into your SQL generation prompt — improving accuracy and
reducing token waste. Built around a multi-file Markdown knowledge base
inspired by Anthropic's Claude skill architecture.

---

## Architecture

```
User Query
    │
    ▼
[Relevance Gate] — classify: SQL query | meta/project question | out-of-domain
    │
    ▼  (SQL query path)
[Section Classifier] — LLM-based: identifies relevant KB sections (ddl, business_rules, …)
    │
    ▼
[Multi-Query Expansion] — 4 query variants generated to improve retrieval recall
    │
    ▼
[pgvector + RRF] — per-section similarity search, Reciprocal Rank Fusion across variants
    │
    ▼
[Cross-Encoder Re-ranking] — LLM batch scoring of top candidates
    │
    ▼
[FK Expansion] — auto-include dimension tables referenced by top-ranked fact tables
    │
    ▼
[Elbow Cutoff] — adaptive score-gap pruning; FK neighbors are protected
    │
    ▼
[Prompt Assembly] — DDL + mandatory SQL guidelines + response format injected
    │
    ▼
[SQL Generation] — dialect-aware LLM call (PostgreSQL / Snowflake / BigQuery / …)
    │
    ▼
[SQL Verifier] — static column/table check against DDL registry (no LLM call)
    │  fails → regenerate once with full column list injected
    ▼
[PEER Layer] — Pre-Execution Entity Resolution: fuzzy-match entity filter values
    │  against DB using pg_trgm; patch SQL before execution
    ▼
[SQL Reviewer] — LLM quality gate: checks completeness, aggregation, human-readable cols
    │  revised SQL re-verified against schema before use
    ▼
[SQL Executor] — executes against remote Postgres; one retry on execution error
    │
    ▼
Result + Knowledge Base Citations
```

---

## Project Structure

```
knowledge_base/
├── main.py                        ← CLI entry point (generate | build | status | query)
├── sql_generator.py               ← LLM SQL generation + retry logic
├── requirements.txt
├── pyproject.toml
│
├── knowledge_base_files/          ← Human-editable Markdown knowledge base
│   ├── KB.md                      ← Root entry point
│   ├── ddl/
│   │   ├── KB.md                  ← Section entry point
│   │   └── <table_name>.md        ← One file per table (frontmatter + DDL body)
│   ├── business_rules/
│   │   ├── KB.md
│   │   └── project_information.md
│   ├── sql_guidelines/
│   │   ├── KB.md
│   │   ├── joins.md               ← Always injected (mandatory)
│   │   ├── filters.md             ← Always injected (mandatory)
│   │   ├── aggregations.md
│   │   ├── comparisons.md
│   │   ├── date_handling.md
│   │   └── performance.md
│   └── response_guidelines/
│       ├── KB.md
│       └── response_format.md     ← Always injected
│
├── kb_system/                     ← Knowledge base pipeline modules
│   ├── kb_parser.py               ← YAML frontmatter + body parsing
│   ├── kb_embeddings.py           ← OpenAI embedding generation
│   ├── kb_store.py                ← Postgres + pgvector storage
│   ├── kb_generator.py            ← LLM-based .md file generation
│   ├── kb_builder.py              ← disk → embed → Postgres (change detection)
│   ├── kb_retriever.py            ← Multi-stage retrieval pipeline
│   └── peer.py                    ← Pre-Execution Entity Resolution
│
├── sql_validator/                 ← Post-generation SQL validation
│   ├── sql_verifier.py            ← Static schema check (sqlglot AST, no LLM)
│   ├── sql_reviewer.py            ← LLM quality review gate
│   └── schema_linker.py           ← DDL → column registry builder
│
├── utils/
│   ├── config.py                  ← All credentials and settings  ← READ THIS FIRST
│   ├── prompt_builder.py          ← SQL prompt assembly
│   ├── citation_builder.py        ← KB retrieval citation formatter
│   ├── llm_client.py              ← Provider-agnostic LLM client (OpenAI / Anthropic)
│   ├── fetch_ddl.py               ← DDL extraction helper
│   ├── sample_values_for_testing.py ← NBA DDL dict used by main.py
│   └── prompts/
│       └── kb_generation_prompts.py ← LLM prompts for KB file generation
│
└── unit_tests/
    ├── test_sql_verifier.py
    ├── test_sql_reviewer.py
    └── test_peer_patch.py
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

> `requirements.txt` includes: `openai`, `psycopg2-binary`, `pgvector`,
> `pyyaml`, `rapidfuzz`, `sqlparse`, `sqlglot`, and a local `.whl` package.

### 2. Configure `utils/config.py`

**This is the most important setup step.**  Open `utils/config.py` and fill in:

```python
# OpenAI
OPENAI_API_KEY = "sk-..."
OPENAI_GENERATION_MODEL = "gpt-4o"   # used for KB generation
OPENAI_SQL_MODEL = "gpt-4o"          # used for SQL generation + review
OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"

# Local Postgres — stores KB embeddings (pgvector required)
db_config = {
    "host": "localhost",
    "port": 5432,
    "username": "postgres",
    "password": "your_password",
    "database": "knowledge_base",
}

# Remote Postgres — the actual data warehouse your SQL runs against
nba_db_config = {
    "host": "your_host",
    "port": 5432,
    "username": "your_user",
    "password": "your_password",
    "database": "your_database",
    "schema": "your_schema",
}
```

All values can also be set via environment variables (e.g. `OPENAI_API_KEY`,
`SQL_DIALECT`, `PEER_ENABLED`, `SQL_REVIEWER_ENABLED`). See the full list of
env-var overrides at the bottom of `utils/config.py`.

### 3. Set up local Postgres for KB storage

The knowledge base embeddings are stored in a **local** Postgres database
(separate from your data warehouse). You need `pgvector` and `pg_trgm`
extensions installed:

```sql
-- Run as superuser on your LOCAL Postgres instance
CREATE DATABASE knowledge_base;
\c knowledge_base
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

> `pg_trgm` is used by the PEER layer for fuzzy entity matching.
> Without it, PEER falls back to prefix-based probing automatically.

### 4. Add your DDL

Open `utils/sample_values_for_testing.py` and update `Sample_NBA_DDL_DICT`
with your actual table DDL:

```python
Sample_NBA_DDL_DICT = {
    "your_table": "CREATE TABLE your_table (...)",
    # paste the output of your DDL extraction script here
}
```

> You can use `utils/fetch_ddl.py` as a starting point to extract DDL from
> your database automatically.

---

## Workflow

Run these commands in order the first time:

```bash
# 1. Generate .md files from your DDL using GPT-4o
python main.py generate

# 2. Review and improve the generated files — especially the 'description' field
#    in each table's frontmatter. Retrieval quality depends on it.
#    Example: knowledge_base_files/ddl/dwh_f_player_boxscore.md

# 3. Embed descriptions and load everything into local Postgres
python main.py build

# 4. Verify what is stored
python main.py status

# 5. Run a query end-to-end
python main.py query "Who scored the most points last season?"
```

### After editing any `.md` file

```bash
python main.py build
# Detects changes automatically:
#   - frontmatter changed  → re-embeds + upserts
#   - body only changed    → upserts content, reuses existing embedding
#   - unchanged            → skipped
```

---

## SQL Query Pipeline (what happens on `query`)

| Stage | What it does | LLM calls |
|---|---|---|
| Relevance gate | Classifies query: SQL / meta / out-of-domain | 1 |
| Section classifier | Selects KB sections to search | 1 |
| Multi-query expansion | Generates 4 query variants for better recall | 1 |
| pgvector + RRF | Similarity search + rank fusion across variants | 0 |
| Cross-encoder | Batch-scores candidates for relevance | 1 |
| FK expansion | Adds referenced dimension tables | 0 |
| SQL generation | Generates dialect-aware SQL | 1 |
| SQL verifier | Checks columns against DDL registry | 0 |
| SQL verifier retry | Regenerates with full column list injected | 0–1 |
| PEER | Fuzzy-matches entity filter values against DB | 1 |
| SQL reviewer | Quality review — completeness, aggregation, IDs | 1 |
| SQL executor | Executes; retries once on error | 0–1 |

Typical total LLM calls per query: **6–8**.

---

## Configuration Reference (`utils/config.py`)

| Variable | Default | Description |
|---|---|---|
| `OPENAI_API_KEY` | — | Required. Your OpenAI API key |
| `OPENAI_SQL_MODEL` | `gpt-4o` | Model for SQL generation and review |
| `OPENAI_EMBEDDING_MODEL` | `text-embedding-3-small` | Embedding model |
| `SQL_DIALECT` | `postgresql` | Target DB dialect. Env: `SQL_DIALECT` |
| `DEFAULT_TOP_K` | `3` | Tables returned per retrieval section |
| `SIMILARITY_THRESHOLD` | `0.40` | Minimum cosine similarity to include a table |
| `PEER_ENABLED` | `true` | Toggle PEER entity resolution. Env: `PEER_ENABLED` |
| `PEER_AUTO_THRESHOLD` | `75` | Auto-substitute fuzzy match score threshold |
| `PEER_FLAG_THRESHOLD` | `60` | Substitute-with-warning score threshold |
| `PEER_USE_TRIGRAM` | `true` | Use pg_trgm for candidate lookup; fallback to prefix ILIKE |
| `SQL_REVIEWER_ENABLED` | `true` | Toggle LLM review gate. Env: `SQL_REVIEWER_ENABLED` |
| `SQL_REVIEWER_MODEL` | `gpt-4o` | Model used for SQL review |
| `overwrite` | `False` | If `True`, regenerate `.md` files even if they exist |
| `MANDATORY_FILES` | `joins.md, filters.md` | SQL guideline files always injected |
| `ALWAYS_INJECT_SECTIONS` | `response_guidelines` | Sections always included in every prompt |

---

## Key Design Decisions

### Why a multi-file knowledge base?
Each table, SQL pattern category, and business rule lives in its own `.md`
file with YAML frontmatter. This means you can edit, version, and regenerate
individual files without touching the rest of the system. The frontmatter
`description` field is what gets embedded — the body (raw DDL, examples,
rules) is injected as-is into the prompt.

### Why two Postgres databases?
- **Local Postgres** stores KB embeddings (pgvector). This is fast, cheap,
  and completely separate from your production data warehouse.
- **Remote Postgres** (or Snowflake, BigQuery, etc.) is your actual data.
  SQL generated by the system runs against this database.

### Why embed only the frontmatter description?
The body of each `.md` file (DDL, SQL examples) is too long and too specific
to embed usefully. The description is a short, semantically rich routing
signal: `"Use when the query involves player performance metrics…"`. At query
time, the user query embedding is compared against these descriptions to find
the most relevant tables. The body is then retrieved and injected verbatim.

### Why mandatory files for joins and filters?
`joins.md` and `filters.md` contain foundational SQL constraints that apply
to almost every query (correct join columns, ILIKE patterns, column ownership
table). Relying on semantic similarity alone sometimes excludes them when
other guideline files score higher. Making them mandatory ensures the LLM
always has the most critical constraints in context.

### The description field is everything
Retrieval quality is almost entirely determined by how well you write the
`description` field in each table's frontmatter. After GPT-4o generates the
initial files, spend time improving these descriptions.

**Good:**
```yaml
description: "Use when the query involves player performance statistics,
  per-game averages, shooting percentages, rebounds, assists, points,
  steals, blocks, turnovers, or individual player box score data.
  Also use for queries about starters vs bench players, or plus/minus."
```

**Weak:**
```yaml
description: "Player statistics table."
```

---

## Running Tests

```bash
python -m pytest unit_tests/ -q
```

Tests cover: SQL verifier (column validation, UNION checks, GROUP BY),
SQL reviewer (response parsing, schema guard), and PEER SQL patching
(sqlparse token-level replacement, wildcard handling, nested queries).
