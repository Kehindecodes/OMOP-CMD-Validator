import pytest
import pandas as pd
from src.validator import Validator

MOCK_SCHEMA = {
    'tables': [
        {
            'name': 'person',
            'primary_key': 'person_id',
            'required_columns': ['person_id', 'gender_concept_id'],
            'datatypes': {
                'person_id': 'int',
                'year_of_birth': 'int',
                'birth_datetime': 'datetime64[ns]'
            }
        },
        {
            'name': 'condition_occurrence',
            'primary_key': 'condition_occurrence_id',
            'required_columns': ['person_id'],
            'datatypes': {'person_id': 'int'},
            'foreign_keys': [
                {'column': 'person_id', 'references': 'person.person_id'}
            ]
        }
    ]
}

@pytest.fixture
def mock_validator():
    """Provides a fresh Validator instance with a mock schema for each test."""
    # Note: Since we haven't implemented loading the schema from file in the test,
    # we'll directly inject the MOCK_SCHEMA into a dummy Validator object.
    # A real implementation would involve mocking the file read.
    validator = Validator(schema_file='dummy_path')
    validator.schema = MOCK_SCHEMA
    validator.validation_report = {'errors': []}
    return validator

def test_validate_required_columns_success(mock_validator):
    """Tests that a clean table passes the NOT NULL check."""
    df = pd.DataFrame({
        'person_id': [1, 2, 3],
        'gender_concept_id': [8507, 8532, 8507]
    })
    table_info = MOCK_SCHEMA['tables'][0]

    # Run the method to be tested
    mock_validator.validate_required_columns(df, table_info)

    # Assert that no errors were recorded
    assert len(mock_validator.validation_report['errors']) == 0

def test_validate_required_columns_failure(mock_validator):
    """Tests that a table with a missing required value generates an error."""
    df = pd.DataFrame({
        'person_id': [1, None, 3],  # person_id is NOT NULL
        'gender_concept_id': [8507, 8532, 8507]
    })
    table_info = MOCK_SCHEMA['tables'][0]

    mock_validator.validate_required_columns(df, table_info)

    # Assert that an error specific to the missing primary key was generated
    assert any("Error: Missing value in required column 'person_id'" in error for error in mock_validator.validation_report['errors'])

def test_validate_primary_key_failure(mock_validator):
    """Tests for duplicate primary keys."""
    df = pd.DataFrame({
        'person_id': [1, 2, 1], # Duplicate '1'
        'gender_concept_id': [8507, 8532, 8507]
    })
    table_info = MOCK_SCHEMA['tables'][0]

    mock_validator.validate_primary_key(df, table_info)

    # Assert for the duplicate PK error
    assert any("Error: Duplicate values found in primary key 'person_id'" in error for error in mock_validator.validation_report['errors'])

def test_validate_foreign_keys_failure(mock_validator):
    """Tests that missing person_id in a child table generates an error."""
    # Set up mock dataframes in the validator's storage
    mock_validator.all_dataframes['person'] = pd.DataFrame({
        'person_id': [1, 2, 3]  # Only these IDs are valid parents
    })

    child_df = pd.DataFrame({
        'person_id': [1, 4], # '4' is the invalid foreign key
        'condition_occurrence_id': [100, 101]
    })

    table_info = MOCK_SCHEMA['tables'][1] # condition_occurrence

    mock_validator.validate_foreign_keys(child_df, table_info)

    # Assert for the foreign key error
    assert any("Error: 1 foreign key(s) in 'person_id'" in error for error in mock_validator.validation_report['errors'])

def test_validate_datatypes_datetime_failure(mock_validator):
    """Tests that an improperly formatted date/time causes an error."""
    df = pd.DataFrame({
        'person_id': [1, 2, 3],
        'birth_datetime': ['2000/01/01', '1990-05-05 12:00:00', 'invalid_date']
    })

    table_info = MOCK_SCHEMA['tables'][0]

    # We must explicitly try to convert types to trigger the error,
    # as the validate_datatypes method contains the try/except logic.
    mock_validator.validate_datatypes(df, table_info)

    # Assert that an error specific to the datatype conversion was generated
    assert any("Error: Data in column 'birth_datetime' of table 'person' could not be converted" in error for error in mock_validator.validation_report['errors'])