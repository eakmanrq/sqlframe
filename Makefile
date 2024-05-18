install-dev:
	pip install -e ".[dev,duckdb,postgres,redshift,bigquery,snowflake,spark]"

install-pre-commit:
	pre-commit install

slow-test:
	pytest -n auto tests

fast-test:
	pytest -n auto tests/unit

local-test:
	pytest -n auto -m "local"

bigquery-test:
	pytest -n auto -m "bigquery"

duckdb-test:
	pytest -n auto -m "duckdb"

style:
	pre-commit run --all-files

docs-serve:
	mkdocs serve

stubs:
	stubgen sqlframe/bigquery/functions.py --output ./ --inspect-mode
	stubgen sqlframe/duckdb/functions.py --output ./ --inspect-mode
	stubgen sqlframe/postgres/functions.py --output ./ --inspect-mode
