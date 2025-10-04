from os import name
from validator.validator import Validator
import pytest
import pandas as pd


@pytest.fixture
def mock_validator():
    validator = Validator("test/mock_schema.yaml")
    validator.validation_report = {"errors": []}
    return validator


def test_load_data_success(mock_validator):
    mock_validator.load_data("data")
    table_infos = mock_validator.schema["tables"]
    for table_info in table_infos:
        assert table_info["name"] in mock_validator.all_dataframes.keys()


def test_load_data_failure(mock_validator):
    mock_validator.load_data("data/src")
    table_infos = mock_validator.schema["tables"]
    for table_info in table_infos:
        assert (
            f" {table_info['name']}.csv not found."
            in mock_validator.validation_report["errors"]
        )


def test_validate_required_columns_success(mock_validator):
    df = pd.DataFrame(
        {
            "person_id": [1, 2, 1],
            "gender_concept_id": [8507, 8532, 8507],
        }
    )
    mock_validator.all_dataframes["person"] = df
    table_info = mock_validator.schema["tables"][0]
    mock_validator.validate_required_columns(df, table_info)
    assert len(mock_validator.validation_report["errors"]) == 0


def test_validate_required_columns_failure(mock_validator):
    df = pd.DataFrame({
        'person_id': [1, None, 3],
        'gender_concept_id': [8507, 8532, 8507]
    })
    table_info = mock_validator.schema['tables'][0]
    mock_validator.all_dataframes['person'] = df
    mock_validator.validate_required_columns(df, table_info)
    assert any(
        'primary key' in error.lower() or 'duplicate' in error.lower()
        for error in mock_validator.validation_report['errors']
    )

def test_validate_primary_key_failure(mock_validator):
    df = pd.DataFrame({
        'person_id': [1, 2, 1],
        'gender_concept_id': [8507, 8532, 8507]
    })
    table_info = mock_validator.schema['tables'][0]
    mock_validator.all_dataframes['person'] = df
    mock_validator.validate_primary_key(df, table_info)
    assert len(mock_validator.validation_report['errors']) > 0

def test_validate_foreign_keys_failure(mock_validator):
    mock_validator.all_dataframes['person'] = pd.DataFrame({
        'person_id': [1, 2, 3]
    })

    child_df = pd.DataFrame({
        'person_id': [1, 4],
        'condition_occurrence_id': [100, 101]
    })

    table_info = mock_validator.schema['tables'][1]

    mock_validator.validate_foreign_keys(child_df, table_info)

    assert len(mock_validator.validation_report['errors']) > 0

# def test_validate_datatypes_datetime_failure(mock_validator):
#     """Tests that an improperly formatted date/time causes an error."""
#     df = pd.DataFrame({
#         'person_id': [1, 2, 3],
#         'birth_datetime': ['2000/01/01', '1990-05-05 12:00:00', 'invalid_date']
#     })

#     table_info = MOCK_SCHEMA['tables'][0]

#     # We must explicitly try to convert types to trigger the error,
#     # as the validate_datatypes method contains the try/except logic.
#     mock_validator.validate_datatypes(df, table_info)

#     # Assert that an error specific to the datatype conversion was generated
#     assert any("Error: Data in column 'birth_datetime' of table 'person' could not be converted" in error for error in mock_validator.validation_report['errors'])
