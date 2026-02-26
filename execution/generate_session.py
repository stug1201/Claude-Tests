#!/usr/bin/env python3
"""
One-time helper script to generate a Telethon StringSession.

Run this locally once to get a session string, then store it in:
  - Your .env file (TELEGRAM_SESSION_STRING)
  - GitHub Actions secrets

This script is interactive and requires a phone number + Telegram verification code.
It should NEVER be run in CI/CD — only locally.

Usage:
    python execution/generate_session.py
"""

import sys

try:
    from telethon.sync import TelegramClient
    from telethon.sessions import StringSession
except ImportError:
    print("Error: telethon is not installed.")
    print("Install it with: pip install telethon")
    sys.exit(1)


def main():
    print("=" * 60)
    print("Telethon Session String Generator")
    print("=" * 60)
    print()
    print("This will generate a session string for headless Telegram access.")
    print("You will need your Telegram API credentials from https://my.telegram.org")
    print()

    # Prompt for credentials
    api_id = input("Enter your API ID: ").strip()
    api_hash = input("Enter your API Hash: ").strip()

    if not api_id or not api_hash:
        print("Error: API ID and API Hash are required.")
        sys.exit(1)

    try:
        api_id = int(api_id)
    except ValueError:
        print("Error: API ID must be a number.")
        sys.exit(1)

    print()
    print("A verification code will be sent to your Telegram account.")
    print()

    # Create client with empty StringSession to generate a new one
    with TelegramClient(StringSession(), api_id, api_hash) as client:
        session_string = client.session.save()

    print()
    print("=" * 60)
    print("SESSION STRING GENERATED SUCCESSFULLY")
    print("=" * 60)
    print()
    print("Your session string (copy this entire line):")
    print()
    print(session_string)
    print()
    print("Add this to your .env file as:")
    print(f"  TELEGRAM_SESSION_STRING={session_string}")
    print()
    print("And to GitHub Actions secrets as TELEGRAM_SESSION_STRING")
    print()
    print("IMPORTANT: Keep this string secret. Anyone with it can")
    print("access your Telegram account.")
    print("=" * 60)


if __name__ == "__main__":
    main()
