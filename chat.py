#!/usr/bin/env python3
"""Simple CLI for chatting with a local Ollama model."""

from __future__ import annotations

import argparse
import sys

from ollama_client import chat, ensure_ollama_running, list_models


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Chat with a local Ollama model.")
    parser.add_argument(
        "prompt",
        nargs="?",
        help="Single prompt to send. Omit for interactive mode.",
    )
    parser.add_argument(
        "-m",
        "--model",
        default="llama3.2",
        help="Model name (default: llama3.2)",
    )
    parser.add_argument(
        "-s",
        "--system",
        help="Optional system prompt",
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Stream the response token by token",
    )
    parser.add_argument(
        "--list-models",
        action="store_true",
        help="List installed models and exit",
    )
    return parser


def print_stream(chunks) -> str:
    parts: list[str] = []
    for chunk in chunks:
        print(chunk, end="", flush=True)
        parts.append(chunk)
    print()
    return "".join(parts)


def interactive_loop(model: str, system: str | None, stream: bool) -> int:
    print(f"Chatting with {model}. Type 'exit' or Ctrl+C to quit.")
    while True:
        try:
            prompt = input("\nYou: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0

        if not prompt:
            continue
        if prompt.lower() in {"exit", "quit"}:
            return 0

        try:
            result = chat(prompt, model=model, system=system, stream=stream)
            print("Assistant: ", end="", flush=True)
            if stream:
                print_stream(result)
            else:
                print(result)
        except RuntimeError as exc:
            print(exc, file=sys.stderr)
            return 1


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        ensure_ollama_running()
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 1

    if args.list_models:
        for name in list_models():
            print(name)
        return 0

    if args.prompt:
        try:
            result = chat(
                args.prompt,
                model=args.model,
                system=args.system,
                stream=args.stream,
            )
            if args.stream:
                print_stream(result)
            else:
                print(result)
        except RuntimeError as exc:
            print(exc, file=sys.stderr)
            return 1
        return 0

    return interactive_loop(args.model, args.system, args.stream)


if __name__ == "__main__":
    raise SystemExit(main())
