## Introduction

Python script for validating data integrity and structure in PostgreSQL databases. This allows you to run configurable checks against PostgreSQL databases using JSON configurations. Can be useful for validating environments after a database restore, ensuring that expected data and structure are in place. It currently supports the following validation types:

- **TABLE_EXISTS**: Ensure that required tables are present in the database schema.
- **COUNT_CHECK**: Run a `SELECT COUNT(*)` query and compare the result to a specified threshold.
- **PRINTER**: Run a query and log the resulting rows for manual inspection.
- **INCLUDES_CHECK**: Verify that specific rows or values are included in the results of a query.
---

## Configuration

An example `config.json` is available in the repository.

The script accepts a JSON config file that defines connection settings and validation checks for one or more databases.

Validation types and required parameters:
- `TABLE_EXISTS`: `required_tables`
- `COUNT_CHECK`: `query`, `operator`, `value`  
  (Supported operators: `=`, `!=`, `>`, `<`, `>=`, `<=`)
- `PRINTER`: `query`
- `INCLUDES_CHECK`: `query`, `values`
---

## How to Run

Database used: https://github.com/defilippomattia/demo-db

```bash
pip install psycopg[binary]
python pg-data-validator.py --config ./config.json
```