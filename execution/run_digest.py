#!/usr/bin/env python3
"""
Orchestration script for the daily crypto news digest.

Runs each stage in sequence:
  1. Scrape Telegram channels
  2. Process and summarise content
  3. Compile daily brief
  4. Send brief to Telegram

Usage:
    python execution/run_digest.py          # Full production run
    python execution/run_digest.py --test   # Test mode with fixtures
"""

import subprocess
import sys
import logging
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("digest_orchestrator")


# Pipeline stages in execution order
STAGES = [
    {
        "name": "Scrape Telegram Channels",
        "script": "execution/scrape_telegram.py",
        "critical": True,  # If this fails, abort entire pipeline
    },
    {
        "name": "Process Content",
        "script": "execution/process_content.py",
        "critical": False,  # Individual item failures are handled inside the script
    },
    {
        "name": "Compile Brief",
        "script": "execution/compile_brief.py",
        "critical": True,
    },
    {
        "name": "Send Brief",
        "script": "execution/send_brief.py",
        "critical": True,
    },
]


def run_stage(stage: dict, test_mode: bool) -> bool:
    """
    Run a single pipeline stage.

    Args:
        stage: Stage config dict with name, script, critical flag.
        test_mode: If True, pass --test flag to the script.

    Returns:
        True if stage succeeded, False otherwise.
    """
    name = stage["name"]
    script = stage["script"]

    logger.info(f"--- Starting stage: {name} ---")
    start_time = datetime.now(timezone.utc)

    cmd = [sys.executable, script]
    if test_mode:
        cmd.append("--test")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout per stage
        )

        # Log stdout/stderr from the stage
        if result.stdout.strip():
            for line in result.stdout.strip().split("\n"):
                logger.info(f"  [{name}] {line}")
        if result.stderr.strip():
            for line in result.stderr.strip().split("\n"):
                logger.warning(f"  [{name}] {line}")

        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()

        if result.returncode == 0:
            logger.info(f"--- Completed stage: {name} ({elapsed:.1f}s) ---")
            return True
        else:
            logger.error(
                f"--- Stage failed: {name} (exit code {result.returncode}, {elapsed:.1f}s) ---"
            )
            return False

    except subprocess.TimeoutExpired:
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.error(f"--- Stage timed out: {name} ({elapsed:.1f}s) ---")
        return False
    except FileNotFoundError:
        logger.error(f"--- Stage script not found: {script} ---")
        return False
    except Exception as e:
        logger.error(f"--- Stage error: {name} — {e} ---")
        return False


def main():
    test_mode = "--test" in sys.argv

    mode_label = "TEST MODE" if test_mode else "PRODUCTION"
    logger.info(f"====== Daily Crypto Digest — {mode_label} ======")
    logger.info(f"Started at {datetime.now(timezone.utc).isoformat()}")

    total_start = datetime.now(timezone.utc)
    failed_stages = []

    for stage in STAGES:
        success = run_stage(stage, test_mode)

        if not success:
            failed_stages.append(stage["name"])

            if stage["critical"]:
                logger.error(
                    f"Critical stage '{stage['name']}' failed. Aborting pipeline."
                )
                break
            else:
                logger.warning(
                    f"Non-critical stage '{stage['name']}' had issues. Continuing."
                )

    total_elapsed = (datetime.now(timezone.utc) - total_start).total_seconds()

    if failed_stages:
        logger.error(
            f"====== Digest completed with errors ({total_elapsed:.1f}s) ======"
        )
        logger.error(f"Failed stages: {', '.join(failed_stages)}")
        sys.exit(1)
    else:
        logger.info(
            f"====== Digest completed successfully ({total_elapsed:.1f}s) ======"
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
