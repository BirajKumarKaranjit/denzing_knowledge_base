"""
kb_system/kb_embeddings.py
---------------------------
Thin wrapper around OpenAI's text-embedding API.

Responsibilities:
    - Compute a single embedding for a text string (get_embedding)
    - Batch-compute embeddings for multiple texts (get_embeddings_batch)
    - Provide a consistent interface so swapping to another embedding
      provider (e.g., Cohere, Voyage AI) only requires changing this file.

Why embeddings?
    The retrieval system compares a user's query against pre-computed
    table description embeddings stored in Postgres. By representing both
    query and descriptions as high-dimensional vectors, we can rank tables
    by semantic similarity rather than keyword overlap — which is essential
    for NBA queries where users might say "scoring leaders" and we need
    to match against "box_scores" whose description says "player performance
    statistics per game including points, rebounds, assists".

Embedding model choice:
    text-embedding-3-small (1536 dims) is used by default. It is:
    - Fast: ~100ms per call
    - Cheap: $0.02 per 1M tokens
    - Accurate enough for this use case (table routing, not doc similarity)
    Upgrade to text-embedding-3-large (3072 dims) for higher accuracy at
    higher cost if retrieval quality needs improvement.
"""

from __future__ import annotations

import time
from typing import Optional

import openai

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import OPENAI_API_KEY, OPENAI_EMBEDDING_MODEL

# Module-level client — created once, reused across all calls in a process.
# This avoids the overhead of creating a new client on every embedding call.
_client = openai.OpenAI(api_key=OPENAI_API_KEY)


def get_embedding(
    text: str,
    model: str = OPENAI_EMBEDDING_MODEL,
    retry_attempts: int = 3,
    retry_delay_seconds: float = 1.0,
) -> list[float]:
    """
    Compute a single text embedding using the OpenAI embeddings API.

    Automatically retries on transient API errors (rate limits, network issues)
    using exponential backoff. This makes the function safe to call in a loop
    when processing many table descriptions during KB build.

    Parameters
    ----------
    text : str
        The text to embed. For KB table files, this should be
        ParsedKBFile.embedding_text (name + description concatenated).
        For user queries at retrieval time, this is the raw query string.
    model : str
        OpenAI embedding model name. Defaults to OPENAI_EMBEDDING_MODEL
        from config.py (text-embedding-3-small). Must match the model
        used when building the KB — mixing models produces incorrect
        similarity scores.
    retry_attempts : int
        Maximum number of retry attempts on API failure. After exhausting
        retries, the last exception is re-raised to the caller.
    retry_delay_seconds : float
        Base delay between retries in seconds. Each subsequent retry doubles
        the delay (exponential backoff): 1s, 2s, 4s, ...

    Returns
    -------
    list[float]
        Dense vector of floats. Length is determined by the model:
        - text-embedding-3-small → 1536 dimensions
        - text-embedding-3-large → 3072 dimensions
        This must match the EMBEDDING_DIMENSION in config.py and the
        vector column dimension in the kb_files Postgres table.

    Raises
    ------
    openai.OpenAIError
        If the API call fails after all retry attempts.
    ValueError
        If text is empty — embedding an empty string wastes an API call
        and produces a meaningless near-zero vector.
    """
    if not text or not text.strip():
        raise ValueError(
            "Cannot embed empty text. Check that KB file descriptions are non-empty."
        )

    last_exception: Optional[Exception] = None

    for attempt in range(1, retry_attempts + 1):
        try:
            response = _client.embeddings.create(
                model=model,
                input=text.strip(),
            )
            return response.data[0].embedding

        except openai.RateLimitError as exc:
            last_exception = exc
            wait = retry_delay_seconds * (2 ** (attempt - 1))
            print(f"[kb_embeddings] Rate limit hit (attempt {attempt}/{retry_attempts}). "
                  f"Waiting {wait:.1f}s...")
            time.sleep(wait)

        except openai.APIConnectionError as exc:
            last_exception = exc
            wait = retry_delay_seconds * (2 ** (attempt - 1))
            print(f"[kb_embeddings] Connection error (attempt {attempt}/{retry_attempts}). "
                  f"Waiting {wait:.1f}s...")
            time.sleep(wait)

        except openai.OpenAIError:
            # Non-retryable errors (auth failure, invalid model, etc.) — raise immediately
            raise

    raise last_exception  # type: ignore[misc]


def get_embeddings_batch(
    texts: list[str],
    model: str = OPENAI_EMBEDDING_MODEL,
) -> list[list[float]]:
    """
    Compute embeddings for a batch of texts in a single API call.

    OpenAI's embeddings endpoint accepts a list of inputs, making batching
    more efficient than calling get_embedding() in a loop. Use this during
    KB build when computing embeddings for all table descriptions at once.

    The API enforces a per-request token limit (~8191 tokens per input).
    For typical KB descriptions (100-500 tokens each), a batch of 20-30
    is safe. For very large descriptions, fall back to get_embedding().

    Parameters
    ----------
    texts : list[str]
        List of text strings to embed. Each string will produce one embedding.
        For KB builds, this would be one entry per table file description.
        Empty strings are replaced with a single space to avoid API errors.
    model : str
        OpenAI embedding model name. Must be consistent across all KB operations
        (build time and query time) since mixing models invalidates comparisons.

    Returns
    -------
    list[list[float]]
        List of embedding vectors in the same order as the input texts.
        Each vector has the same dimension as the model produces.

    Raises
    ------
    openai.OpenAIError
        If the API call fails.
    ValueError
        If texts is empty.
    """
    if not texts:
        raise ValueError("texts list is empty — nothing to embed.")

    # Replace empty strings with a space to avoid API validation errors
    sanitized = [t.strip() if t and t.strip() else " " for t in texts]

    response = _client.embeddings.create(
        model=model,
        input=sanitized,
    )

    # The API returns embeddings in the same order as the input.
    # Sort by index to be safe (API spec guarantees order but let's be explicit).
    sorted_data = sorted(response.data, key=lambda item: item.index)
    return [item.embedding for item in sorted_data]

