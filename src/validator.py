import pandas as pd
import yaml

class Validator:
    def __init__(self, schema_file):
        with open(schema_file, 'r') as file:
            self.schema = yaml.safe_load(file)
        self.all_dataframes = {}
        self.validation_report = {'errors': []}

    def load_data(self, data_path):
        """Loads all CSV files defined in the schema into dataframes."""
        for table in self.schema['tables']:
            table_name = table['name']
            file_path = f"{data_path}/{table_name}.csv"
            try:
                self.all_dataframes[table_name] = pd.read_csv(file_path, low_memory=False)
                print(f"Loaded {table_name}.csv successfully.")
            except FileNotFoundError:
                error_msg = f" {table_name}.csv not found."
                self.validation_report['errors'].append(error_msg)
                print(error_msg)

    def validate_all(self):
        """Runs all validation checks."""
        for table in self.schema['tables']:
            table_name = table['name']
            if table_name in self.all_dataframes:
                df = self.all_dataframes[table_name]
                print(f"Starting validation for table: {table_name}")

                # Run structural checks
                self.validate_required_columns(df, table)
                self.validate_datatypes(df, table)
                self.validate_primary_key(df, table)

                # Run relational checks
                self.validate_foreign_keys(df, table)

    def validate_required_columns(self, df, table):
        required_cols = table.get('required_columns', [])
        for col in required_cols:
          if col in df.columns:
            if df[col].isnull().any():
                    error_msg = f" A value is not provided for required column '{col}' of table '{table['name']}'."
                    self.validation_report['errors'].append(error_msg)
            continue
        error_msg = f" The '{col}' field is missing in '{table['name']}' table."
        self.validation_report['errors'].append(error_msg)

    def validate_datatypes(self, df, table):
        datatypes = table.get('datatypes', {})
        for col, expected_dtype in datatypes.items():
            if col in df.columns:
                if str(df[col].dtype) != expected_dtype:
                    error_msg = f" Data type mismatch in column '{col}' of table '{table['name']}': expected '{expected_dtype}', got '{df[col].dtype}'."
                    self.validation_report['errors'].append(error_msg)

    def validate_primary_key(self, df, table):
        primary_key = table.get('primary_key')
        if primary_key in df.columns:
            if primary_key and df[primary_key].duplicated().any():
                number_of_duplicates = df[primary_key].value_counts()
                error_msg = f"'df{primary_key}' is duplicated '{number_of_duplicates}' times in '{table['name']}' table."
                self.validation_report['errors'].append(error_msg)

    def validate_foreign_keys(self, df, table):
        foreign_keys = table.get('foreign_keys', [])
        for foreign_key in foreign_keys:
            foreign_key_col = foreign_key['column']
            references = foreign_key['references'].split('.')
            reference_table, reference_primary_key = references[0], references[1]

            if foreign_key_col in df.columns and reference_table in self.all_dataframes:
                reference_df = self.all_dataframes[reference_table]
                reference_id_list = list(df[foreign_key_col])
                for reference_id in reference_id_list:
                  reference = reference_df.loc[reference_df[reference_primary_key]==reference_id]
                  if not reference:
                        error_msg = f"The {foreign_key_col} in  table '{table['name']} do not exist in {reference_table} table  "
                        self.validation_report['errors'].append(error_msg)


    def get_report(self):
        """Returns the final validation report."""
        return self.validation_report

# Example of how to use the Validator class
# if __name__ == '__main__':
#     validator = Validator('config/omop_schema.yaml')
#     validator.load_data('data')
    # validator.validate_all()
  # report = validator.get_report()

    # print("\n--- Validation Report ---")
    # if report['errors']:
    #     print("Validation Failed with the following errors:")
    #     for error in report['errors']:
    #         print(f"- {error}")
    # else:
    #     print("Validation successful! No errors found.")