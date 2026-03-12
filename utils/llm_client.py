"""
Single place for constructing LLM clients and making chat completions calls.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

_log = logging.getLogger(__name__)


def get_llm_client(provider: Literal["openai", "anthropic"], api_key: str) -> Any:
    """Construct and return an LLM client for *provider*.

    Parameters
    ----------
    provider:
        ``"openai"`` or ``"anthropic"``.
    api_key:
        API key for the chosen provider.

    Returns
    -------
    Any
        ``openai.OpenAI`` or ``anthropic.Anthropic`` instance.
    """
    if provider == "openai":
        import openai
        return openai.OpenAI(api_key=api_key)

    if provider == "anthropic":
        import anthropic
        return anthropic.Anthropic(api_key=api_key)

    raise ValueError(f"Unsupported provider: {provider!r}. Choose 'openai' or 'anthropic'.")


def call_llm(
    client: Any,
    model: str,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 1000,
    temperature: float = 0.0,
) -> str:
    """Make a single chat completion call and return the response text.
    Parameters
    ----------
    client:
        Client returned by ``get_llm_client()``.
    model:
        Model identifier string (e.g. ``"gpt-4o"`` or ``"claude-3-5-sonnet-20241022"``).
    system_prompt:
        System-level instructions for the model.
    user_prompt:
        User-facing message content.
    max_tokens:
        Maximum tokens in the response. Defaults to 1000.
    temperature:
        Sampling temperature. Defaults to 0.0 for determinism.

    Returns
    -------
    str
        Plain response text from the model.
    """
    import openai as _openai

    # OpenAI client
    if isinstance(client, _openai.OpenAI):
        from openai.types.chat import (
            ChatCompletionSystemMessageParam,
            ChatCompletionUserMessageParam,
        )
        response = client.chat.completions.create(
            model=model,
            messages=[
                ChatCompletionSystemMessageParam(role="system", content=system_prompt),
                ChatCompletionUserMessageParam(role="user", content=user_prompt),
            ],
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return (response.choices[0].message.content or "").strip()

    # Anthropic client — detected by class name to avoid a hard import at module level
    if type(client).__name__ == "Anthropic":
        response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        for block in response.content:
            if getattr(block, "type", None) == "text":
                return block.text.strip()
        return ""

    raise TypeError(
        f"Unrecognised client type: {type(client).__name__}. "
        "Use get_llm_client() to construct the client."
    )

