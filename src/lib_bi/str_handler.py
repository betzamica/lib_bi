import os


def gen_query_part_table(
    name_table_and_partition: tuple[str, int],
    partition_format_table: str,
) -> set:
    queries_and_path = {}
    for tuple_part_schem in name_table_and_partition:
        name_table, range_partition = tuple_part_schem

        table_query_and_path = tuple(
            (partition_format_table.format(partition=partition, name_table=name_table),)
            for partition in range(range_partition + 1)
        )

        queries_and_path[name_table] = table_query_and_path

    return queries_and_path


def get_all_sql_files(path_sql: str, remove_file: str = "") -> dict:
    """
    Reads all SQL files in a directory and returns a dictionary with file names as keys and queries as values.
    """
    sqls_name = os.listdir(path_sql)
    if remove_file:
        sqls_name = sqls_name.remove(remove_file)
    sql_dict = {}

    for sql_file in sqls_name:
        path = os.path.join(path_sql, sql_file)
        with open(path, encoding="utf-8") as f:
            sql = f.read()
        sql_name = sql_file.replace(".sql", "")
        sql_dict[sql_name] = sql

    return sql_dict
