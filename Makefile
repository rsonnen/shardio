.DEFAULT_GOAL := help
.PHONY: help install format format-check lint typecheck test test-slow all build publish clean

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-18s %s\n", $$1, $$2}'

install: ## Build the venv (uv sync)
	uv sync

format: ## Format src and tests in place
	uv run ruff format src tests

format-check: ## Fail if src or tests are not formatted
	uv run ruff format --check src tests

lint: ## ruff check
	uv run ruff check src tests

typecheck: ## mypy --strict on src and tests
	uv run mypy

test: ## Unit tests (skips the slow marker)
	uv run pytest

test-slow: ## Large-disk / long-running tests only
	uv run pytest -m slow

all: format-check lint typecheck test ## Every quality gate

build: ## Build sdist and wheel into dist/
	uv build

publish: all build ## Run every gate, build, then upload to PyPI (token from UV_PUBLISH_TOKEN)
	uv publish

clean: ## Remove caches
	rm -rf .pytest_cache .ruff_cache .mypy_cache
