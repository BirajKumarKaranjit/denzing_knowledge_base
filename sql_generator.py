"""
sql_generator.py
----------------
Calls the OpenAI chat API to generate SQL from the assembled prompt.

This is the final step in the pipeline:
    KB retrieval → prompt assembly → LLM call → SQL output

Kept deliberately thin — all the intelligence is in the KB retrieval
and prompt assembly steps. This module just makes the API call and
returns the raw SQL string.
"""

from __future__ import annotations

import re
import sys
import os

# sql_generator.py lives at the project root — add it to sys.path so
# that `utils.config` can be imported regardless of how the script is invoked.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import openai
from openai.types.chat import ChatCompletionSystemMessageParam, ChatCompletionUserMessageParam

from utils.config import OPENAI_API_KEY, OPENAI_SQL_MODEL

_client = openai.OpenAI(api_key=OPENAI_API_KEY)


def generate_sql(prompt: str, temperature: float = 0.25) -> str:
    """
    Send the assembled prompt to the LLM and return the generated SQL.

    Uses temperature=0.0 by default for maximum determinism — SQL generation
    benefits from consistency over creativity. The LLM should return a
    markdown code block containing valid SQL.

    Parameters
    ----------
    prompt : str
        Fully assembled prompt from prompt_builder.build_sql_prompt().
        Contains schema context, guidelines, and the user question.
    temperature : float
        LLM sampling temperature. 0.0 = fully deterministic (recommended
        for SQL). Increase to 0.2-0.5 for more varied responses during
        experimentation.

    Returns
    -------
    str
        Raw LLM response text. Typically contains a SQL code block
        (e.g., a fenced sql block with SELECT ...).
        Use extract_sql_from_response() to get just the SQL string.

    Raises
    ------
    openai.OpenAIError
        If the API call fails.
    """
    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(
            role="system",
            content="You are an expert SQL analyst. Return ONLY the SQL query in a markdown code block.",
        ),
        ChatCompletionUserMessageParam(
            role="user",
            content=prompt,
        ),
    ]
    response = _client.chat.completions.create(
        model=OPENAI_SQL_MODEL,
        messages=messages,
        temperature=temperature,
    )
    return response.choices[0].message.content or ""


def extract_sql_from_response(llm_response: str) -> str:
    """
    Extract the raw SQL string from an LLM response containing a code block.

    Handles both fenced sql blocks (with or without the sql language tag)
    and plain responses with no code block fencing.

    Parameters
    ----------
    llm_response : str
        Raw string returned by generate_sql().

    Returns
    -------
    str
        Clean SQL string with no markdown fencing.
    """

    # Match ```sql ... ``` or ``` ... ``` blocks
    match = re.search(r"```(?:sql)?\s*\n?(.*?)```", llm_response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()

    # Fallback: return the whole response stripped
    return llm_response.strip()
