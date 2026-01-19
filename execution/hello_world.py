#!/usr/bin/env python3
"""
Hello World Example Script

A demonstration script showing the execution layer pattern.
This script generates a personalized greeting and saves it to a file.

Usage:
    python hello_world.py --name "Alice" --output ".tmp/greeting.txt"

Inputs:
    --name: The name to greet (default: "World")
    --output: Output file path (default: ".tmp/greeting.txt")

Outputs:
    - Text file with personalized greeting
    - Exit code 0 on success, 1 on failure
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path


def generate_greeting(name: str) -> str:
    """Generate a personalized greeting message."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"Hello, {name}!\n\nGenerated at: {timestamp}\n"


def save_greeting(greeting: str, output_path: str) -> None:
    """Save the greeting to a file, creating directories if needed."""
    path = Path(output_path)

    # Create parent directories if they don't exist
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write the greeting
    path.write_text(greeting)
    print(f"Greeting saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate a personalized greeting message"
    )
    parser.add_argument(
        "--name",
        type=str,
        default="World",
        help="The name to greet (default: World)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=".tmp/greeting.txt",
        help="Output file path (default: .tmp/greeting.txt)"
    )

    args = parser.parse_args()

    try:
        # Generate the greeting
        greeting = generate_greeting(args.name)

        # Save to file
        save_greeting(greeting, args.output)

        # Print success
        print("Success!")
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
