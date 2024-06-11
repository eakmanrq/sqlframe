from setuptools import find_packages, setup

setup(
    name="sqlframe",
    description="Turning PySpark Into a Universal DataFrame API",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/eakmanrq/sqlframe",
    author="Ryan Eakman",
    author_email="eakmanrq@gmail.com",
    license="MIT",
    packages=find_packages(include=["sqlframe", "sqlframe.*"]),
    package_data={"sqlframe": ["py.typed", "*.pyi", "**/*.pyi"]},
    use_scm_version={
        "write_to": "sqlframe/_version.py",
        "fallback_version": "0.0.0",
        "local_scheme": "no-local-version",
    },
    setup_requires=["setuptools_scm"],
    python_requires=">=3.8",
    install_requires=[
        "prettytable<3.11.0",
        "sqlglot>=24.0.0,<25.1",
        "typing_extensions>=4.8,<5",
    ],
    extras_require={
        "bigquery": [
            "google-cloud-bigquery[pandas]>=3,<4",
            "google-cloud-bigquery-storage>=2,<3",
        ],
        "dev": [
            "duckdb>=0.9,<1.1",
            "mypy>=1.10.0,<1.11",
            "openai>=1.30,<1.34",
            "pandas>=2,<3",
            "pandas-stubs>=2,<3",
            "psycopg>=3.1,<4",
            "pyarrow>=10,<17",
            "pyspark>=2,<3.6",
            "pytest>=8.2.0,<8.3",
            "pytest-postgresql>=6,<7",
            "pytest-xdist>=3.6,<3.7",
            "pre-commit>=3.5;python_version=='3.8'",
            "pre-commit>=3.7,<3.8;python_version>='3.9'",
            "ruff>=0.4.4,<0.5",
            "types-psycopg2>=2.9,<3",
        ],
        "docs": [
            "mkdocs==1.4.2",
            "mkdocs-include-markdown-plugin==6.0.6",
            "mkdocs-material==9.0.5",
            "mkdocs-material-extensions==1.1.1",
            "pymdown-extensions",
        ],
        "duckdb": [
            "duckdb>=0.9,<1.1",
            "pandas>=2,<3",
        ],
        "openai": [
            "openai>=1.30,<1.34",
        ],
        "pandas": [
            "pandas>=2,<3",
        ],
        "postgres": [
            "psycopg2>=2.8,<3",
        ],
        "redshift": [
            "redshift_connector>=2.1.1,<2.2.0",
        ],
        "snowflake": [
            "snowflake-connector-python[secure-local-storage]>=3.10.0,<3.11",
        ],
        "spark": [
            "pyspark>=2,<3.6",
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
