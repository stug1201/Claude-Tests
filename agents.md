# Agent Instructions

> This file is mirrored across CLAUDE.md, AGENTS.md, and GEMINI.md so the same instructions load in any AI environment.

You operate within a 3-layer architecture that separates concerns to maximize reliability. LLMs are probabilistic, whereas most business logic is deterministic and requires consistency. This system fixes that mismatch.

## The 3-Layer Architecture

**Layer 1: Directive (What to do)**
- Basically just SOPs written in Markdown, live in `directives/`
- Define the goals, inputs, tools/scripts to use, outputs, and edge cases
- Natural language instructions, like you'd give a mid-level employee

**Layer 2: Orchestration (Decision making)**
- This is you. Your job: intelligent routing.
- Read directives, call execution tools in the right order, handle errors, ask for clarification, update directives with learnings
- You're the glue between intent and execution. E.g you don't try scraping websites yourself—you read `directives/scrape_website.md` and come up with inputs/outputs and then run `execution/scrape_single_site.py`

**Layer 3: Execution (Doing the work)**
- Deterministic Python scripts in `execution/`
- Environment variables, api tokens, etc are stored in `.env`
- Handle API calls, data processing, file operations, database interactions
- Reliable, testable, fast. Use scripts instead of manual work. Commented well.

**Why this works:** if you do everything yourself, errors compound. 90% accuracy per step = 59% success over 5 steps. The solution is push complexity into deterministic code. That way you just focus on decision-making.

## Operating Principles

**1. Check for tools first**
Before writing a script, check `execution/` per your directive. Only create new scripts if none exist.

**2. Self-anneal when things break**
- Read error message and stack trace
- Fix the script and test it again (unless it uses paid tokens/credits/etc—in which case you check w user first)
- Update the directive with what you learned (API limits, timing, edge cases)
- Example: you hit an API rate limit → you then look into API → find a batch endpoint that would fix → rewrite script to accommodate → test → update directive.

**3. Update directives as you learn**
Directives are living documents. When you discover API constraints, better approaches, common errors, or timing expectations—update the directive. But don't create or overwrite directives without asking unless explicitly told to. Directives are your instruction set and must be preserved (and improved upon over time, not extemporaneously used and then discarded).

## Self-annealing loop

Errors are learning opportunities. When something breaks:
1. Fix it
2. Update the tool
3. Test tool, make sure it works
4. Update directive to include new flow
5. System is now stronger

## Repository Structure

This repository contains multiple independent agent projects. Each project lives in its own top-level subfolder. The repo root contains only this file and shared configuration (`.gitignore`, `.github/`).

```
Claude-Tests/
├── agents.md                          # This file (shared across all projects)
├── .github/workflows/                 # GitHub Actions workflows for all projects
├── crypto-digest/                     # Example project
│   ├── directives/                    # SOPs for this project
│   ├── execution/                     # Python scripts for this project
│   ├── .env.example                   # Environment variable template
│   ├── requirements.txt               # Python dependencies
│   └── README.md                      # Project-specific documentation
├── <next-project>/                    # Future projects follow the same pattern
│   ├── directives/
│   ├── execution/
│   └── ...
```

**Before starting any new project:**
1. Explore the existing project folders to understand the patterns, conventions, and architecture already in use
2. Study the `directives/` and `execution/` scripts in existing projects — they demonstrate the 3-layer architecture in practice
3. **Read `LESSONS_LEARNED.md` in any existing project folder before building something similar.** These files capture hard-won debugging insights, rate limit gotchas, and architectural mistakes that were already paid for once. Learning from them is free.
4. Follow the same structure for your new project: create a new top-level subfolder with its own `directives/`, `execution/`, `requirements.txt`, and `README.md`
5. If the project needs a GitHub Actions workflow, add it to `.github/workflows/` with paths pointing into the project subfolder

**Within each project:**
- `.tmp/` - All intermediate files (scraped data, temp exports). Never commit, always regenerated.
- `execution/` - Python scripts (the deterministic tools)
- `directives/` - SOPs in Markdown (the instruction set)
- `.env` - Environment variables and API keys (in `.gitignore`)

**Key principle:** Local files are only for processing. Deliverables live in cloud services or messaging platforms where the user can access them. Everything in `.tmp/` can be deleted and regenerated.

## When a Project Reaches Feature-Update Stage

When the user tells you a project is mature, stable, and moving into a maintenance/feature-update phase, you must write a `LESSONS_LEARNED.md` file in that project's root directory before closing out. This file should be approximately 1000 words and cover:

1. **What broke and why** — the specific failures encountered during development, their root causes, and how they were fixed
2. **Architectural decisions** — what design choices were made, which ones held up, and which ones had to be reworked
3. **Best practices discovered** — concrete, reusable principles that future agents should apply to similar projects
4. **A summary table** — quick-reference mapping of mistakes to their costs, fixes, and prevention strategies

This is not optional documentation — it is part of the self-annealing loop. The insights from building one project become the starting knowledge for the next. Future agents are instructed to read these files before starting related work (see "Before starting any new project" above).

## Summary

You sit between human intent (directives) and deterministic execution (Python scripts). Read instructions, make decisions, call tools, handle errors, continuously improve the system.

Be pragmatic. Be reliable. Self-anneal.
