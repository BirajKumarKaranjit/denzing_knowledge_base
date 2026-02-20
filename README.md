# NBA Knowledge Base System for Text2SQL

Semantic retrieval layer that injects only **relevant** table schemas into
your SQL generation prompt — improving accuracy and reducing token waste.

## Architecture

```
User Query
    │
    ▼
[Stage 1] Section Classifier (keyword match, free)
    │  → identifies: ddl, business_rules
    ▼
[Stage 2] pgvector Similarity Search (within section only)
    │  → top-3 tables: box_scores, players, games
    ▼
[Prompt Assembly] Inject relevant DDL + business rules + guidelines
    │
    ▼
GPT-4 SQL Generation
    │
    ▼
Executable SQL
```

## Project Structure

```
your_project/
├── knowledge_base/          ← .md files (human-editable)
│   ├── KB.md
│   ├── ddl/
│   │   ├── KB.md
│   │   ├── players.md
│   │   ├── box_scores.md
│   │   └── ...
│   ├── business_rules/KB.md
│   ├── sql_guidelines/KB.md
│   └── response_guidelines/KB.md
├── kb_system/               ← KB Python modules
│   ├── kb_parser.py         ← Parse .md frontmatter
│   ├── kb_embeddings.py     ← OpenAI embeddings
│   ├── kb_store.py          ← Postgres + pgvector
│   ├── kb_generator.py      ← GPT-4 .md generation
│   ├── kb_builder.py        ← disk → embed → Postgres
│   └── kb_retriever.py      ← Two-stage retrieval
├── sql_pipeline/
│   ├── prompt_builder.py    ← Assemble SQL prompt
│   └── sql_generator.py     ← GPT-4 SQL generation
├── config.py                ← All credentials + settings
├── main.py                  ← CLI entry point
└── requirements.txt
```

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Install pgvector on Postgres
```sql
-- Connect to your database as superuser:
CREATE EXTENSION IF NOT EXISTS vector;
```

### 3. Configure credentials
Edit `config.py`:
```python
OPENAI_API_KEY = "sk-..."           # Your OpenAI API key
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "nba_db",
    "user": "your_user",
    "password": "your_password",
}
```
Or set environment variables: `OPENAI_API_KEY`, `POSTGRES_HOST`, etc.

### 4. Add your DDL
In `main.py`, update `NBA_DDL_DICT` with your actual table DDL:
```python
NBA_DDL_DICT = {
    "players": "CREATE TABLE players (...)",
    # output of your DDL extraction script
}
```

## Workflow

### First-time setup (run once):
```bash
# Generate .md files from your DDL
python main.py generate

# IMPORTANT: Review and improve the generated files, especially 'description' fields
# vim knowledge_base_files/ddl/players.md

# Embed + load into Postgres
python main.py build
```

### Verify everything loaded:
```bash
python main.py status
```

### Test end-to-end:
```bash
python main.py query "Who scored the most points last season?"
python main.py test   # runs all TEST_QUERIES in main.py
```

### After editing any .md file:
```bash
python main.py build  # re-embeds and upserts (safe to run anytime)
```

## Key Design Decisions

### Why section scoping?
Instead of searching ALL KB files, we classify the query to a section
(e.g., "ddl") and search only within it. For an NBA schema with 8 tables,
this means 8 embedding comparisons, not 50+.

### Why embed name + description?
`"players: Use when query involves player stats, points, assists..."`
encodes both the identifier and semantics. Queries like "show me LeBron's
stats" match "players" on name; "show me scoring leaders" match on description.

### Why not embed KB.md entry points?
KB.md files are section-level guides. They're injected alongside matched
table files for context, but they're not retrieval targets — we retrieve
at the table level, not the section level.

### The description field is everything
Retrieval quality is almost entirely determined by how well you write
the `description` field in each table's frontmatter. After GPT-4 generates
the initial files, spend time improving these descriptions.

Good description:
```yaml
description: "Use when the query involves player performance statistics,
  per-game averages, shooting percentages, rebounds, assists, points,
  steals, blocks, turnovers, or individual player box score data.
  Also use for queries about starters vs bench players, or plus/minus."
```

Weak description:
```yaml
description: "Player statistics table."
```
