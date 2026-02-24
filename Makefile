lock:
	uv lock

install-dev:
	uv sync --all-extras

install-pre-commit:
	pre-commit install

slow-test:
	pytest -n auto tests

fast-test:
	pytest -n auto tests/unit

local-test:
	pytest -n auto -m "fast or local" --reruns 5

%-test:
	pytest -n auto -m "${*}"

style:
	pre-commit run --all-files

docs-serve:
	mkdocs serve

stubs:
	stubgen sqlframe/bigquery/functions.py --output ./ --inspect-mode
	stubgen sqlframe/duckdb/functions.py --output ./ --inspect-mode
	stubgen sqlframe/postgres/functions.py --output ./ --inspect-mode
	stubgen sqlframe/snowflake/functions.py --output ./ --inspect-mode
	stubgen sqlframe/databricks/functions.py --output ./ --inspect-mode
	stubgen sqlframe/spark/functions.py --output ./ --inspect-mode

package:
	uv build

publish: package
	uv publish
