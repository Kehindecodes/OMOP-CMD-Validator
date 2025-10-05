import pandas as pd
import yaml
import numpy as np
import time


class Validator:
    """A class for validating data against an OMOP CDM schema.

    The Validator class provides functionality to load data from CSV files and validate it against
    a specified schema in YAML format. It checks for required tables, columns, and data types
    as defined in the schema.

    Attributes:
        schema (dict): The loaded YAML schema containing table and column definitions.
        all_dataframes (dict): Dictionary to store loaded dataframes with table names as keys.
        validation_report (dict): Dictionary containing error messages and validation results.
    """

    def __init__(self, schema_file):
        with open(schema_file, "r", encoding="utf-8") as file:
            self.schema = yaml.safe_load(file)
        self.all_dataframes = {}
        self.validation_report = {"errors": []}

    def load_data(self, data_path):
        """Loads all CSV files defined in the schema into dataframes."""
        for table in self.schema["tables"]:
            table_name = table["name"]
            file_path = f"{data_path}/{table_name}.csv"
            try:
                self.all_dataframes[table_name] = pd.read_csv(
                    file_path, low_memory=False
                )
                print(f"Loaded {table_name}.csv successfully.")
            except FileNotFoundError:
                error_msg = f" {table_name}.csv not found."
                self.validation_report["errors"].append(error_msg)
                print(error_msg)

    def validate_all(self):
        """Runs all validation checks."""
        for table in self.schema["tables"]:
            table_name = table["name"]
            if table_name in self.all_dataframes:
                df = self.all_dataframes[table_name]
                print(f"Starting structural validation for table: {table_name}......")

                # Run structural checks
                step_start = time.time()
                self.validate_required_columns(df, table)
                print(
                    f"Finished required columns validation for table: {table_name}, it took  {round(time.time() - step_start, 2)} seconds"
                )
                step_start = time.time()
                self.validate_datatypes(df, table)
                print(
                    f"Finished datatypes validation for table: {table_name}, it took {round(time.time() - step_start, 2)} seconds"
                )
                step_start = time.time()
                self.validate_primary_key(df, table)
                print(
                    f"Finished primary key validation for table: {table_name}, it took {round(time.time() - step_start, 2)} seconds"
                )

                step_start = time.time()
                self.validate_character_length(df, table)
                print(
                    f"Finished character length validation for table: {table_name}, it took {round(time.time() - step_start, 2)} seconds"
                )

                # Run relational checks
                step_start = time.time()
                print(f"Starting relational validation for table: {table_name}......")
                self.validate_foreign_keys(df, table)
                print(
                    f"Finished foreign keys validation for table: {table_name}, it took {round(time.time() - step_start, 2)} seconds"
                )

                # Run relational checks
                step_time = time.time()
                print(f"Starting relational validation for table: {table_name}......")
                self.validate_foreign_keys(df, table)
                print(
                    f"Finished foreign keys validation for table: {table_name}, it took {round(time.time() - step_time, 2)} seconds"
                )


    def validate_required_columns(self, df, table):
        required_cols = table.get("required_columns", [])
        for col in required_cols:
            if col in df.columns:
                if df[col].isnull().any():
                    error_msg = f" A value is not provided for '{col}'in'{table['name']}'table.And it is required"
                    self.validation_report["errors"].append(error_msg)
            else:
                error_msg = f" The '{col}' field is missing in '{table['name']}' table."
                self.validation_report["errors"].append(error_msg)

    def validate_datatypes(self, df, table):
        datatypes = table.get("datatypes", {})
        for col, expected_dtype in datatypes.items():
            if col in df.columns:
                for index, row in df.iterrows():
                    column_value = row[col]
                    if not pd.isna(column_value):
                        if expected_dtype == "datetime64[ns]":
                            date_in_datetime64 = pd.to_datetime(
                                column_value, errors="coerce"
                            )
                            if date_in_datetime64:
                                column_value = date_in_datetime64
                        actual_type = self._normalize_type(column_value)
                        if type(expected_dtype) == dict:
                            expected_dtype = expected_dtype.get("type")
                        if expected_dtype != actual_type:
                            error_msg = f"Data type mismatch in row {index + 1}, column '{col}' of table '{table['name']}': expected '{expected_dtype}', got '{actual_type}'."
                            self.validation_report["errors"].append(error_msg)

    def validate_character_length(self, df, table):
        datatypes = table.get("datatypes", {})
        for col, expected_dtype in datatypes.items():
            if type(expected_dtype) == dict:
                if expected_dtype.get("type") == "string":
                    if col in df.columns:
                        for index, row in df.iterrows():
                            column_value = row[col]
                            if not pd.isna(column_value):
                                column_value = (
                                    column_value
                                    if type(column_value) == str
                                    else str(column_value)
                                )
                                length = len(column_value)
                                max_length = expected_dtype.get("max_length")
                                if max_length and max_length < length:
                                    error_msg = f"Character length  provided in row {index + 1}, column '{col}' of table '{table['name']}' is more than max length: expected '{max_length}', got '{length}'."
                                    self.validation_report["errors"].append(
                                        error_msg
                                    )

    # def validate_primary_key(self, df, table):
    #     primary_key = table.get("primary_key")
    #     if not primary_key:
    #         return
    #     if primary_key in df.columns:
    #         if primary_key and df[primary_key].duplicated().any():
    #             number_of_duplicates = df[primary_key].value_counts()
    #             error_msg = f"'df{primary_key}' is duplicated '{number_of_duplicates}' times in '{table['name']}' table."
    #             self.validation_report["errors"].append(error_msg)

    def validate_primary_key(self, df, table):
        primary_key = table.get("primary_key")
        if not primary_key or primary_key not in df.columns:
            return

        unique_count = df[primary_key].nunique(dropna=False)
        if len(df) != unique_count:
            # Only compute value_counts if duplicates exist
            number_of_duplicates = df[primary_key].value_counts()
            error_msg = f"'{primary_key}' has {len(df) - unique_count} duplicate(s) in '{table['name']}'. Duplicate counts:\n{number_of_duplicates[number_of_duplicates > 1]}"
            self.validation_report["errors"].append(error_msg)

    def validate_foreign_keys(self, df, table):
        foreign_keys = table.get("foreign_keys", [])
        for foreign_key in foreign_keys:
            foreign_key_col = foreign_key["column"]
            reference = foreign_key["reference"].split(".")
            reference_table, reference_primary_key = reference[0], reference[1]

            if foreign_key_col in df.columns and reference_table in self.all_dataframes:
                reference_df = self.all_dataframes[reference_table]
                invalid_references = ~df[foreign_key_col].isin(
                    reference_df[reference_primary_key]
                )
                if invalid_references.any():
                    invalid_values = df.loc[
                        invalid_references, foreign_key_col
                    ].unique()
                    for value in invalid_values:
                        error_msg = f"Value {value} in column '{foreign_key_col}' of table '{table['name']}' does not exist in the {reference_table} table"
                        self.validation_report["errors"].append(error_msg)

    def get_report(self):
        """Returns the final validation report."""
        return self.validation_report

    def _normalize_type(self, value):
        """Convert value to a normalized type name for comparison."""
        if pd.isna(value):
            return None
        if isinstance(value, (int, np.integer)):
            return "integer"
        if isinstance(value, (float, np.floating)):
            return "float"
        if isinstance(value, (str, np.str_)):
            return "string"
        if isinstance(value, (bool, np.bool_)):
            return "bool"
        if isinstance(value, (pd.Timestamp, np.datetime64)):
            return "datetime64[ns]"
        return type(value).__name__
