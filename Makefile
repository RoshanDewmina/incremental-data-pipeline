.PHONY: setup test demo seed benchmark
setup:
	uv sync --frozen
test:
	uv run pytest -q
seed:
	uv run python3 -m pipeline.cli generate data/demo.ndjson --jobs 100
	uv run python3 -m pipeline.cli ingest data/demo.ndjson
demo:
	uv run python3 -m pipeline.api
benchmark:
	uv run python3 -m pipeline.cli benchmark
