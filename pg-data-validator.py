import json
import logging
import sys
import psycopg
import argparse
import operator

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

OPERATORS = {
    "=": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}

def create_connection(conn_config):
    """
    Create a psycopg3 connection from config dict and set the search_path.
    """
    try:
        conn = psycopg.connect(
            host=conn_config["host"],
            port=conn_config["port"],
            dbname=conn_config["dbname"],
            user=conn_config["user"],
            password=conn_config["password"]
        )
        logging.info("Connected to PostgreSQL database.")
        
        schema = conn_config.get("schema")
        if schema:
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {schema}")
                logging.info(f"Search path set to schema: {schema}")
        
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        sys.exit(1)

def run_printer(cursor, query, description):
    """
    Execute a PRINTER validation type: run query and log rows.
    """
    logging.info("##########################################################################")
    logging.info(f"Running PRINTER: {description}")
    logging.info(f"Query: {query}")
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc.name for desc in cursor.description]
        logging.info(f"{len(rows)} rows returned:")
        for row in rows:
            logging.info(dict(zip(columns, row)))
    except Exception as e:
        logging.error(f"Failed to execute PRINTER query: {e}")

def run_table_exists(cursor, required_tables, schema='public'):
    """
    Check if all required tables exist in the specified schema.
    """
    logging.info("##########################################################################")
    logging.info(f"Running TABLE_EXISTS check for tables: {required_tables}")
    try:
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """,
            (schema,)
        )
        existing_tables = {row[0] for row in cursor.fetchall()}
        missing_tables = [t for t in required_tables if t not in existing_tables]

        if not missing_tables:
            logging.info("All required tables exist!")
            return True
        else:
            logging.error(f"Missing tables: {missing_tables}")
            return False
    except Exception as e:
        logging.error(f"Failed to execute TABLE_EXISTS check: {e}")
        return False

def run_count_check(cursor, query, value, op_str, description="COUNT_CHECK"):
    logging.info("##########################################################################")
    logging.info(f"Running COUNT_CHECK: {description}")
    logging.info(f"Query: {query}")
    logging.info(f"Operator: {op_str}, Value: {value}")

    op_func = OPERATORS.get(op_str)
    if not op_func:
        logging.error(f"Unsupported operator: {op_str}")
        return False

    try:
        cursor.execute(query)
        result = cursor.fetchone()
        if result is None:
            logging.error("Query returned no rows.")
            return False
        actual_value = result[0]
        if op_func(actual_value, value):
            logging.info(f"Check passed: {actual_value} {op_str} {value}")
            return True
        else:
            logging.error(f"Check failed: {actual_value} {op_str} {value} is False")
            return False
    except Exception as e:
        logging.error(f"Failed to execute COUNT_CHECK query: {e}")
        return False
def run_includes_check(cursor, query, expected_values, description="INCLUDES_CHECK"):
    """
    Check if expected values are included in query results.
    """
    logging.info("##########################################################################")
    logging.info(f"Running INCLUDES_CHECK: {description}")
    logging.info(f"Query: {query}")
    logging.info(f"Expected values: {expected_values}")

    try:
        cursor.execute(query)
        actual_rows = cursor.fetchall()
        actual_set = {tuple(row) for row in actual_rows}
        expected_set = {tuple(row) for row in expected_values}

        missing = expected_set - actual_set

        if not missing:
            logging.info("All expected values are present.")
            return True
        else:
            logging.error(f"Missing values: {missing}")
            return False
    except Exception as e:
        logging.error(f"Failed to execute INCLUDES_CHECK: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Postgres Data Validator")
    parser.add_argument(
        "-c", "--config",
        required=True,
        help="Path to validation config JSON file"
    )
    args = parser.parse_args()

    try:
        with open(args.config) as f:
            config = json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file '{args.config}' not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in config file: {e}")
        sys.exit(1)

    for db in config.get("databases", []):
        conn_config = db["connection"]
        validations = db.get("validation", [])

        with create_connection(conn_config) as conn:
            with conn.cursor() as cur:
                for validator in validations:
                    vtype = validator.get("type")
                    if vtype == "PRINTER":
                        run_printer(cur, validator["query"], validator.get("description", "No description"))
                    elif vtype == "TABLE_EXISTS":
                        schema = conn_config.get("schema", "public")
                        required_tables = validator.get("required_tables", [])
                        run_table_exists(cur, required_tables, schema)
                    elif vtype == "COUNT_CHECK":
                        run_count_check(
                            cur,
                            query=validator.get("query"),
                            value=validator.get("value"),
                            op_str=validator.get("operator"),
                            description=validator.get("description", "No description")
                        )
                    elif vtype == "INCLUDES_CHECK":
                        run_includes_check(
                            cur,
                            query=validator.get("query"),
                            expected_values=validator.get("values", []),
                            description=validator.get("description", "No description")
                        )


        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
