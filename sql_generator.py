"""sql_generator.py

Calls the OpenAI chat API to generate SQL from the assembled prompt.

Also provides a query relevance gate that short-circuits the pipeline for
questions that cannot be answered from the database.
"""

from __future__ import annotations

import json
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import openai
from openai.types.chat import ChatCompletionSystemMessageParam, ChatCompletionUserMessageParam

from utils.config import OPENAI_API_KEY, OPENAI_SQL_MODEL
from utils.prompts.kb_generation_prompts import (
    RELEVANCE_CHECK_SYSTEM_PROMPT,
    relevance_check_user_prompt,
)

_client = openai.OpenAI(api_key=OPENAI_API_KEY)


def is_query_relevant(user_query: str, schema_summary: str) -> tuple[bool, str]:
    """Check whether a query is answerable from the database before running the pipeline.

    Parameters
    ----------
    user_query:
        Raw user question.
    schema_summary:
        Brief description of the database domain and available tables.
        Used to ground the relevance decision.

    Returns
    -------
    tuple[bool, str]
        (is_relevant, reason) — if False, pass `reason` back to the user.
    """
    try:
        response = _client.chat.completions.create(
            model=OPENAI_SQL_MODEL,
            messages=[
                ChatCompletionSystemMessageParam(
                    role="system", content=RELEVANCE_CHECK_SYSTEM_PROMPT
                ),
                ChatCompletionUserMessageParam(
                    role="user",
                    content=relevance_check_user_prompt(user_query, schema_summary),
                ),
            ],
            temperature=0.0,
            max_tokens=100,
        )
        raw = response.choices[0].message.content.strip()
        match = re.search(r'\{[^}]+}', raw, re.DOTALL)
        if match:
            data = json.loads(match.group())
            return bool(data.get("is_relevant", True)), str(data.get("reason", "")), data.get("suggested_questions", "")
    except (json.JSONDecodeError, openai.OpenAIError, KeyError):
        pass
    # Fail open — if we cannot parse the response, let the pipeline continue
    return True, "", []


def generate_sql(prompt: str, temperature: float = 0.25) -> str:
    """Send the assembled prompt to the LLM and return the generated SQL.

    Parameters
    ----------
    prompt:
        Fully assembled prompt from prompt_builder.build_sql_prompt().
    temperature:
        LLM sampling temperature. Lower values produce more deterministic SQL.

    Returns
    -------
    str
        Raw LLM response text containing a SQL code block.
    """
    messages: list[ChatCompletionSystemMessageParam | ChatCompletionUserMessageParam] = [
        ChatCompletionSystemMessageParam(
            role="system",
            content="You are an expert SQL analyst. Return ONLY the SQL query in a markdown code block.",
        ),
        ChatCompletionUserMessageParam(role="user", content=prompt),
    ]
    response = _client.chat.completions.create(
        model=OPENAI_SQL_MODEL,
        messages=messages,
        temperature=temperature,
    )
    return response.choices[0].message.content or ""


def extract_sql_from_response(llm_response: str) -> str:
    """Extract the raw SQL string from an LLM response containing a code block.

    Parameters
    ----------
    llm_response:
        Raw string returned by generate_sql().

    Returns
    -------
    str
        Clean SQL string with no markdown fencing.
    """
    match = re.search(r"```(?:sql)?\s*\n?(.*?)```", llm_response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return llm_response.strip()
