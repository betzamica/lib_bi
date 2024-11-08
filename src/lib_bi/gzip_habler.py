import os
import subprocess
import time
import polars as pl
import logging
from typing import Callable


def concatenate_and_compress_tables(
    table_schem: tuple[str, int],
    output_folder: str,
    clean_func: Callable[[str, str], str],
) -> None:
    """
    Concatenates DataFrames from partitioned CSV files and compresses the result into a single gzip file.

    Args:
        table_name (str): The name of the table being processed.
        partition_numbers (range): Range of partition numbers to process.
        output_folder (str): Path to the folder where the concatenated file will be saved.
    """
    for table_name, partition_number in table_schem:
        filename, part_filename = clean_func(table_name)
        concatenated_csv = os.path.join(output_folder, filename)

        if partition_number == 0:
            try:
                df: pl.DataFrame = pl.read_csv(
                    concatenated_csv, infer_schema_length=5000000
                )
            except Exception as error:
                logging.error(
                    f"Failed to convert the csv to a polars dataframes: {error}",
                    stack_info=True,
                )
                raise error
            df.write_csv(concatenated_csv)
            del df

        else:
            with open(concatenated_csv, mode="a") as file:
                for partition_index in range(partition_number + 1):
                    partition_file = os.path.join(output_folder, part_filename).format(
                        partition_number=partition_index
                    )
                    try:
                        df: pl.DataFrame = pl.read_csv(
                            partition_file, infer_schema_length=5000000
                        )
                    except Exception as error:
                        logging.error(
                            f"Failed to convert the csv to a polars dataframes: {error}",
                            stack_info=True,
                        )
                        raise error
                    if partition_index == 0:
                        df.write_csv(file)
                    df.write_csv(file, include_header=False)
                    del df
                    os.remove(partition_file)
                    logging.info(f"The {table_name}_{partition_index} was concatened")

        # --- Compression ---
        start = time.time()
        command = f"gzip --fast {concatenated_csv}"
        result = subprocess.run(command, shell=True)

        if not result.returncode == 0:
            logging.error(f"{concatenated_csv} is empty", stack_info=True)
            raise Exception

        end = time.time()
        execution_time = end - start
        logging.info(f"The file {table_name} compression time was: {execution_time}")
