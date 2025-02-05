install-dev:
	pip install -e ".[bigquery,dev,docs,duckdb,pandas,postgres,redshift,snowflake,databricks,spark]"

install-pre-commit:
	pre-commit install

slow-test:
	pytest -n auto tests

fast-test:
	pytest -n auto tests/unit

local-test:
	pytest -n auto -m "fast or local"

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
	pip3 install wheel && python3 setup.py sdist bdist_wheel

publish: package
	pip3 install twine && python3 -m twine upload dist/*
