from os import name
from validator.validator import Validator
import pytest
import pandas as pd


@pytest.fixture
def mock_validator():
    validator = Validator('test/mock_schema.yaml')
    validator.validation_report = {'errors': []}
    return validator

def test_load_data_success(mock_validator):
     mock_validator.load_data('data')
     table_infos = mock_validator.schema['tables']
     for table_info in table_infos:
         assert table_info['name'] in mock_validator.all_dataframes.keys()

def test_load_data_failure(mock_validator):
        mock_validator.load_data('data/src')
        table_infos = mock_validator.schema['tables']
        for table_info in table_infos:
            assert f" {table_info['name']}.csv not found." in mock_validator.validation_report['errors']

