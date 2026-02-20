---
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
