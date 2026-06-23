# Contributing to dag-lint

Thanks for taking the time to contribute! This project is small and friendly — issues, ideas, and pull requests are all welcome.

## Getting set up

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e . pytest
pytest -v
```

## Ground rules

- **Every change ships with a test.** Bug fix? Add a test that fails before and passes after. New feature? Cover the happy path and at least one edge case.
- **Keep it dependency-light.** A big part of this project's value is being small and easy to drop in. Please discuss before adding a new runtime dependency.
- **Match the existing style.** Small, readable functions; comments that explain *why*, not *what*.
- **CI must be green** before a PR can merge.

## Submitting a pull request

1. Fork and create a branch (`feature/my-thing` or `fix/the-bug`).
2. Make your change with tests.
3. Run the full test suite locally.
4. Open a PR describing **what** changed and **why**. Link any related issue.

## Reporting bugs / requesting features

Use the issue templates — they prompt for the details that make a report actionable. A minimal reproduction is worth a thousand words.

## Code of Conduct

Be kind and constructive. We follow the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).
