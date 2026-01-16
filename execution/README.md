# Execution Scripts

This directory contains deterministic Python scripts that handle the actual work.

## Purpose

Execution scripts are responsible for:
- API calls
- Data processing
- File operations
- Database interactions

## Guidelines

1. **Check before creating**: Always check if a script already exists before writing a new one
2. **Deterministic**: Scripts should produce consistent outputs for the same inputs
3. **Well-commented**: Include docstrings and inline comments
4. **Testable**: Write scripts that can be tested independently
5. **Environment variables**: Use `.env` for API keys and configuration

## Script Template

```python
#!/usr/bin/env python3
"""
Script Name: description of what it does

Usage:
    python script_name.py [arguments]

Inputs:
    - input1: description
    - input2: description

Outputs:
    - output1: description
"""

import os
from dotenv import load_dotenv

load_dotenv()

def main():
    # Implementation here
    pass

if __name__ == "__main__":
    main()
```

## Error Handling

Scripts should:
- Raise clear exceptions with helpful messages
- Log errors appropriately
- Return meaningful exit codes
