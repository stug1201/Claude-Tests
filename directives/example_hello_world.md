# Hello World Example

> This is a sample directive demonstrating the 3-layer architecture pattern.

## Goal

Generate a personalized greeting message and save it to a file.

## Inputs

| Input | Type | Required | Description |
|-------|------|----------|-------------|
| name | string | Yes | The name to greet |
| output_path | string | No | Where to save the greeting (default: `.tmp/greeting.txt`) |

## Execution Script

Use `execution/hello_world.py` to generate the greeting.

```bash
python execution/hello_world.py --name "Alice" --output ".tmp/greeting.txt"
```

## Expected Output

- A text file containing the personalized greeting
- Console output confirming success

## Edge Cases

- **Empty name**: Script will use "World" as default
- **Invalid output path**: Script will raise a clear error message
- **File already exists**: Script will overwrite existing file

## Notes

- This is a demonstration directive
- Use this as a template for creating new directives
