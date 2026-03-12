"""
kb_system/kb_embeddings.py
---------------------------
Thin wrapper around OpenAI's text-embedding API.

Responsibilities:
    - Compute a single embedding for a text string (get_embedding)
    - Batch-compute embeddings for multiple texts (get_embeddings_batch)
    - Provide a consistent interface so swapping to another embedding
"""

from __future__ import annotations

import time
from typing import Optional

import openai

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import OPENAI_API_KEY, OPENAI_EMBEDDING_MODEL

_client = openai.OpenAI(api_key=OPENAI_API_KEY)


def get_embedding(
    text: str,
    model: str = OPENAI_EMBEDDING_MODEL,
    retry_attempts: int = 3,
    retry_delay_seconds: float = 1.0,
) -> list[float]:
    """
    Compute a single text embedding using the OpenAI embeddings API.

    Parameters
    ----------
    text : str
    model : str
    retry_attempts : int
    retry_delay_seconds : float
    Returns
    -------
    list[float]
        Dense vector of floats.

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
            raise

    raise last_exception


def get_embeddings_batch(
    texts: list[str],
    model: str = OPENAI_EMBEDDING_MODEL,
) -> list[list[float]]:
    """
    Compute embeddings for a batch of texts in a single API call.

    Parameters
    ----------
    texts : list[str]
    model : str
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

    sanitized = [t.strip() if t and t.strip() else " " for t in texts]

    response = _client.embeddings.create(
        model=model,
        input=sanitized,
    )
    sorted_data = sorted(response.data, key=lambda item: item.index)
    return [item.embedding for item in sorted_data]

