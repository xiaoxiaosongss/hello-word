# hello-word

A minimal Python example for chatting with [Ollama](https://ollama.com) locally.

## Prerequisites

1. Install Ollama: https://ollama.com/download
2. Start the Ollama service (it usually runs automatically after install)
3. Pull a model:

```bash
ollama pull llama3.2
```

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Send a single prompt:

```bash
python chat.py "Why is the sky blue?"
```

Interactive chat:

```bash
python chat.py
```

Stream tokens as they are generated:

```bash
python chat.py --stream "Write a haiku about coding"
```

Use a different model:

```bash
python chat.py -m gemma3 "Hello"
```

List installed models:

```bash
python chat.py --list-models
```

## Configuration

Point at a remote Ollama instance:

```bash
export OLLAMA_HOST=http://192.168.1.10:11434
python chat.py "Hello from another machine"
```

## Project layout

- `ollama_client.py` — reusable wrapper around the Ollama SDK
- `chat.py` — command-line chat interface
