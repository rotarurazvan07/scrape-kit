# Local development

Use this page only when you work on scrape-kit itself. Integrators should install from git as shown in the [README](../README.md).

## Editable install

From the repository root, with Python 3.11 or newer:

```bash
python -m pip install -e ".[test]"
scrapling install
```

## Tests

```bash
pytest
```

Run a single module when you need a narrower check:

```bash
pytest tests/test_fetcher_webfetcher.py
```
