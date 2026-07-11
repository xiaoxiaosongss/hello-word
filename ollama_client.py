"""Thin wrapper around the Ollama Python SDK."""

from __future__ import annotations

import os
from typing import Iterator

from ollama import Client, ResponseError


def get_client() -> Client:
    host = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
    return Client(host=host)


def list_models(client: Client | None = None) -> list[str]:
    client = client or get_client()
    return [model.model for model in client.list().models]


def chat(
    prompt: str,
    *,
    model: str,
    system: str | None = None,
    stream: bool = False,
    client: Client | None = None,
) -> str | Iterator[str]:
    client = client or get_client()
    messages: list[dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    if stream:
        return _stream_response(client, model, messages)

    response = client.chat(model=model, messages=messages)
    return response.message.content


def _stream_response(
    client: Client, model: str, messages: list[dict[str, str]]
) -> Iterator[str]:
    for chunk in client.chat(model=model, messages=messages, stream=True):
        content = chunk.message.content
        if content:
            yield content


def ensure_ollama_running(client: Client | None = None) -> None:
    client = client or get_client()
    try:
        client.list()
    except ResponseError as exc:
        raise RuntimeError(f"Ollama request failed: {exc}") from exc
    except ConnectionError as exc:
        host = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
        raise RuntimeError(
            f"Cannot reach Ollama at {host}. "
            "Install from https://ollama.com and run `ollama serve`."
        ) from exc
