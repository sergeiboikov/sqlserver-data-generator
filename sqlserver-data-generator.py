import string
import random
import decimal
import time
import pyodbc
import json
from datetime import datetime

INSERT_TEMPLATE = "INSERT INTO [{{schema_name}}].[{{table_name}}]({{columns}}) VALUES ({{values}});"

COLUMN_INFO = {
    "COLUMN_NAME": "columnname",
    "ORDINAL_POSITION": "ordinal_position",
    "IS_NULLABLE": "is_nullable",
    "DATA_TYPE": "data_type",
    "CHARACTER_MAXIMUM_LENGTH": "character_maximum_length",
    "CHARACTER_OCTET_LENGTH": "character_octet_length",
    "NUMERIC_PRECISION": "numeric_precision",
    "NUMERIC_PRECISION_RADIX": "numeric_precision_radix",
    "NUMERIC_SCALE": "numeric_scale",
    "DATETIME_PRECISION": "datetime_precision",
    "CHARACTER_SET_NAME": "character_set_name",
    "COLLATION_NAME": "collation_name"
}


def get_random_string(length: int,
                      pool: str = string.ascii_letters) -> str:
    """
    Returns random string
    length: Length of each token
    pool: Iterable of characters to choose from 
    """

    join = ''.join

    token = join(random.choices(pool, k=length))
    return repr(token)


def get_random_decimal() -> str:
    """
    Returns random decimal number
    """
    number = str(decimal.Decimal(random.randrange(155, 389)) / 100)
    return number


def get_random_smallint() -> str:
    """
    Returns random smallint number
    """
    number = str(random.randint(-32767, 32767))
    return number


def get_random_int() -> str:
    """
    Returns random int number
    """
    number = str(random.randint(-2147483648, 2147483648))
    return number


def str_time_prop(start, end, time_format, prop) -> str:
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formatted in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return repr(time.strftime(time_format, time.localtime(ptime)))


def get_random_datetime(start: str = "1980-01-01",
                        end: str = str(datetime.date(datetime.now()))) -> str:
    """
    Returns random datetime between two data
    :param start: date in format '%Y-%m-%d'
    :type start: str
    :param end: date in format '%Y-%m-%d'
    :type end: str
    :return: date in format '%Y-%m-%d'
    :rtype: str
    """
    return str_time_prop(start, end, '%Y-%m-%d', random.random())


def get_random_binary(length: int) -> bytes:
    """
    Returns random binary from string with length equal length
    :param length: length of string for getting binary value
    :type leghth: int
    :return: binary string
    :rtype: str
    """
    string_for_binary = get_random_string(length)
    return f"CAST({string_for_binary} AS BINARY({length}))"


def generate_random_values_for_columns(columns_info: list) -> dict:
    """
    Return list with random values for columns with properties from columns_info list
    :param columns_info: list of columns with properties
    :type columns_info: list
    :return: Dictionary where key is column name; value is generated random value for the column
    :rtype: dict
    """
    columns_values = []
    for col in columns_info:
        if col[COLUMN_INFO["DATA_TYPE"]] in ("char", "varchar", "nchar", "nvarchar"):
            column_value = get_random_string(length=col[COLUMN_INFO["CHARACTER_MAXIMUM_LENGTH"]])
            columns_values.append({col[COLUMN_INFO["COLUMN_NAME"]]: column_value})
        elif col[COLUMN_INFO["DATA_TYPE"]] == "decimal":
            column_value = get_random_decimal()
            columns_values.append({col[COLUMN_INFO["COLUMN_NAME"]]: column_value})
        elif col[COLUMN_INFO["DATA_TYPE"]] == "binary":
            column_value = get_random_binary(length=col[COLUMN_INFO["CHARACTER_MAXIMUM_LENGTH"]])
            columns_values.append({col[COLUMN_INFO["COLUMN_NAME"]]: column_value})
        elif col[COLUMN_INFO["DATA_TYPE"]] == "smallint":
            column_value = get_random_smallint()
            columns_values.append({col[COLUMN_INFO["COLUMN_NAME"]]: column_value})
        elif col[COLUMN_INFO["DATA_TYPE"]] == "int":
            column_value = get_random_int()
            columns_values.append({col[COLUMN_INFO["COLUMN_NAME"]]: column_value})
        elif col[COLUMN_INFO["DATA_TYPE"]] in ("date", "smalldatetime", "datetime", "datetime2"):
            column_value = get_random_datetime(start="2021-07-09", end="2021-07-09")
            columns_values.append({col[COLUMN_INFO["COLUMN_NAME"]]: column_value})
        else:
            print(f"{col[COLUMN_INFO['COLUMN_NAME']]}({col[COLUMN_INFO['DATA_TYPE']]}): UNKNOWN FORMAT")
    return columns_values


def generatesqlstring(sqltemplate: str, randomvalues: list) -> str:
    """
    Generates SQL string from sqltemplate for passed parameters.
    """
    sql = sqltemplate
    sql = sql.replace('{{random_values}}', randomvalues)
    return sql


def get_tables_info_from_db(server_name: str, db_name: str, schema_name: str, table_names: list) -> dict:
    """
    Return dictionary that contains information about selected tables from database
    :return: dictionary that contains information about selected tables from database
    :rtype: dict
    """

    connection = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                f'Server={server_name};'
                                f'Database={db_name};'
                                'Trusted_Connection=yes;')

    config_dict = {"config": [{"database": db_name, "tables": []}]}
    tables = config_dict["config"][0]["tables"]
    for table_name in table_names:
        table_dict = {"tablename": table_name, "tableschema": schema_name, "columns": []}
        columns = table_dict["columns"]
        query = f"""SELECT COLUMN_NAME,
                           ORDINAL_POSITION,
                           IS_NULLABLE,
                           DATA_TYPE,
                           CHARACTER_MAXIMUM_LENGTH,
                           CHARACTER_OCTET_LENGTH,
                           NUMERIC_PRECISION,
                           NUMERIC_PRECISION_RADIX,
                           NUMERIC_SCALE,
                           DATETIME_PRECISION,
                           CHARACTER_SET_NAME,
                           COLLATION_NAME
                           FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_SCHEMA='{schema_name}'
                           AND TABLE_NAME = '{table_name}'"""
        cursor = connection.cursor()
        cursor.execute(query)
        for row in cursor:
            column_info = {COLUMN_INFO["COLUMN_NAME"]: row[0],
                           COLUMN_INFO["ORDINAL_POSITION"]: row[1],
                           COLUMN_INFO["IS_NULLABLE"]: row[2],
                           COLUMN_INFO["DATA_TYPE"]: row[3],
                           COLUMN_INFO["CHARACTER_MAXIMUM_LENGTH"]: row[4],
                           COLUMN_INFO["CHARACTER_OCTET_LENGTH"]: row[5],
                           COLUMN_INFO["NUMERIC_PRECISION"]: row[6],
                           COLUMN_INFO["NUMERIC_PRECISION_RADIX"]: row[7],
                           COLUMN_INFO["NUMERIC_SCALE"]: row[8],
                           COLUMN_INFO["DATETIME_PRECISION"]: row[9],
                           COLUMN_INFO["CHARACTER_SET_NAME"]: row[10],
                           COLUMN_INFO["COLLATION_NAME"]: row[11]}
            columns.append(column_info)
        tables.append(table_dict)
    return config_dict


def construct_insert_string(schema_name: str, table_name: str, columns_values: dict) -> str:
    """
    Returns generated insert string
    :param schema_name:
    :type schema_name: str
    :param table_name:
    :type table_name: str
    :param columns_values:
    :type columns_values: dict
    :return: Generated sql string for inserting data
    :rtype: str
    """
    # Get column names in square brackets for all columns from dictionary columns_values
    all_cols_str = ", ".join([f"[{list(d.keys())[0]}]" for d in columns_values])
    all_values_str = ", ".join([list(d.values())[0] for d in columns_values])
    sql_string = INSERT_TEMPLATE.replace("{{schema_name}}", schema_name) \
        .replace("{{table_name}}", table_name) \
        .replace("{{columns}}", all_cols_str) \
        .replace("{{values}}", all_values_str)
    return sql_string


def generate_insert_queries(rows_count: int, tables_info: dict) -> list:
    """
    Returns list of insert queries
    :param rows_count: Number of rows for generating test data
    :type rows_count: int
    :param tables_info: Dictionary that contains info about selected tables
    :type tables_info: dict
    :return: List of sql queries for inserting data
    :rtype: list
    """
    insert_queries = []
    for table_info in tables_info["config"][0]["tables"]:
        table_name = table_info["tablename"]
        columns_info = table_info["columns"]
        for i in range(rows_count):
            columns_values = generate_random_values_for_columns(columns_info)
            insert_string = construct_insert_string(schema_name, table_name, columns_values)
            insert_queries.append(insert_string)
    return insert_queries


def export_queries_to_sql_file(queries: list, sql_file_path: str):
    """
    Exports queries from list of queries to sql file
    :param queries: List of queries for export to SQL file
    :type queries: list
    :param sql_file_path: Path for output SQL file with queries
    :type sql_file_path: str
    """
    with open(sql_file_path, 'w', encoding='utf-8') as sqlf:
        for query in queries:
            sqlf.write(f"{query}\n")


def export_tables_info_to_json(tables_info: dict, json_file_path: str):
    """
    Exports table info from dict to json file
    :param table_info: Dictionary with info about table
    :type table_info: dict
    :param json_file_path: Path for output json file with table info
    :type json_file_path: str
    """
    with open(json_file_path, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(tables_info))


if __name__ == "__main__":
    server_name = "localhost"
    # Input database for populating test data
    db_name = "test_db"
    # Input schema for populating test data
    schema_name = "dbo"
    # Input table for populating test data
    table_names = ["test_table1", "test_table2"]
    # Output JSON file with metadata for table
    json_file_path = r"output/table_info.json"

    # Output SQL file with queries for inserting test data
    sql_file_path = f"output/{db_name}_test_data_population.sql"

    # Get tables info from DB
    tables_info = get_tables_info_from_db(server_name=f"{server_name}",
                                          db_name=f"{db_name}",
                                          schema_name=f"{schema_name}",
                                          table_names=table_names)

    # Export table info to JSON file
    export_tables_info_to_json(tables_info, json_file_path)

    # Get generated queries for inserting data
    generated_insert_queries = generate_insert_queries(rows_count=1, tables_info=tables_info)

    # Export queries to SQL file
    export_queries_to_sql_file(generated_insert_queries, sql_file_path)
