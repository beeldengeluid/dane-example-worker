from moto import mock_aws
import boto3
#import main_data_processor
from base_util import LOG_FORMAT
import logging
import sys
from dane.config import cfg
import pytest
import os
from io_util import validate_s3_uri, obtain_input_file, generate_output_dirs


source_id = "resource__carrier"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def aws(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")

@pytest.fixture
def create_bucket1(aws):
    client = boto3.client("s3")
    client.create_bucket(Bucket="bucket1")
    client.put_object(Bucket="bucket1", Key=f"assets/prep__{source_id}.tar.gz")


@pytest.fixture
def setup_fs():
    generate_output_dirs(source_id)


def test_io_util(aws, aws_credentials, create_bucket1, setup_fs):
    #from io_util import validate_s3_uri, obtain_input_file
    s3_uri = f"s3://bucket1/assets/prep__{source_id}.tar.gz"
    assert validate_s3_uri(s3_uri=s3_uri)

    client = boto3.client("s3")

    with open(f'data/input-files/{source_id}/prep__{source_id}.tar.gz', "wb") as f:
        client.download_fileobj("bucket1", f"assets/prep__{source_id}.tar.gz", f)
    # The above seems to work here, but not from within the module...

    model_input = obtain_input_file(s3_uri=s3_uri)
    assert model_input.state == 200


