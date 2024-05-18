from setuptools import find_packages, setup

setup(
    name="sqlframe",
    description="Taking the Spark out of PySpark by converting to SQL",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/eakmanrq/sqlframe",
    author="Ryan Eakman",
    author_email="eakmanrq@gmail.com",
    license="MIT",
    packages=find_packages(include=["sqlframe", "sqlframe.*"]),
    package_data={"sqlframe": ["py.typed"]},
    use_scm_version={
        "write_to": "sqlframe/_version.py",
        "fallback_version": "0.0.0",
        "local_scheme": "no-local-version",
    },
    setup_requires=["setuptools_scm"],
    python_requires=">=3.8",
    install_requires=[
        "prettytable",
        "sqlglot",
    ],
    extras_require={
        "bigquery": [
            "google-cloud-bigquery[pandas]",
            "google-cloud-bigquery-storage",
            "pandas",
        ],
        "dev": [
            "duckdb",
            "mypy",
            "pandas",
            "pandas-stubs",
            "psycopg",
            "pyarrow",
            "pyspark",
            "pytest",
            "pytest-postgresql",
            "pytest-xdist",
            "pre-commit",
            "ruff",
            "typing_extensions",
            "types-psycopg2",
        ],
        "docs": [
            "mkdocs==1.4.2",
            "mkdocs-include-markdown-plugin==4.0.3",
            "mkdocs-material==9.0.5",
            "mkdocs-material-extensions==1.1.1",
            "pymdown-extensions",
        ],
        "duckdb": [
            "duckdb",
            "pandas",
        ],
        "postgres": [
            "pandas",
            "psycopg2",
        ],
        "redshift": [
            "pandas",
            "redshift_connector",
        ],
        "snowflake": [
            "pandas",
            "snowflake-connector-python[pandas,secure-local-storage]",
        ],
        "spark": [
            "pyspark",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
