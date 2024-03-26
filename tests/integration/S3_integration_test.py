from moto import mock_aws
import boto3
from main_data_processor import run
from dane.config import cfg
import pytest
import os
import tarfile
from io_util import validate_s3_uri, obtain_input_file, generate_output_dirs


source_id = "resource__carrier"
fn_tar = f"prep__{source_id}.tar.gz"
example_key = f"{cfg.INPUT.S3_FOLDER_IN_BUCKET}/{fn_tar}"

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"  # Other regions make stuff complex
    os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = cfg.INPUT.S3_ENDPOINT_URL


@pytest.fixture(scope="function")
def aws(aws_credentials):
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture(scope="function")
def create_sample_input():
    fn = '{source_id}.input'
    with open(fn, 'w') as f:
        f.write('This is just a file with some random input')
    with tarfile.open(fn_tar, 'w:gz') as tar: 
        tar.add(fn)
    yield
    #  after test: cleanup
    os.remove(fn)
    os.remove(fn_tar)

@pytest.fixture
def create_and_fill_buckets(aws, create_sample_input):
    client = boto3.client("s3")
    for bucket in [cfg.INPUT.S3_BUCKET, cfg.OUTPUT.S3_BUCKET, cfg.INPUT.S3_BUCKET_MODEL]:
        client.create_bucket(Bucket=bucket)
    client.put_object(Body=fn_tar,  # "tests/integration/resource__carrier.input",
                      Bucket=cfg.INPUT.S3_BUCKET,
                      Key=f"{cfg.INPUT.S3_FOLDER_IN_BUCKET}/prep__{source_id}.tar.gz")


@pytest.fixture
def setup_fs():
    generate_output_dirs(source_id)

"""
def test_io_util(aws, aws_credentials, create_and_fill_buckets, setup_fs):
    s3_uri = f"s3://{cfg.INPUT.S3_BUCKET}/{cfg.INPUT.S3_FOLDER_IN_BUCKET}/prep__{source_id}.tar.gz"
    assert validate_s3_uri(s3_uri=s3_uri)

    client = boto3.client("s3")

    with open(f'data/input-files/{source_id}/prep__{source_id}.tar.gz', "wb") as f:
        client.download_fileobj(cfg.INPUT.S3_BUCKET, example_key, f)

    model_input = obtain_input_file(s3_uri=s3_uri)
    assert model_input.state == 200
"""


def test_main_data_processor(aws, aws_credentials, create_and_fill_buckets):
    assert True
    if cfg.OUTPUT.TRANSFER_ON_COMPLETION:
        # run the main data processor
        run(input_file_path=f"s3://{cfg.INPUT.S3_BUCKET}/{cfg.INPUT.S3_FOLDER_IN_BUCKET}/prep__{source_id}.tar.gz")
        # Check if the output is present in S3
        client = boto3.client("s3")
        found = False
        for item in client.list_objects(Bucket=cfg.OUTPUT.S3_BUCKET)['Contents']:
            found = item['Key'] == f'{cfg.OUTPUT.S3_FOLDER_IN_BUCKET}/resource__carrier/base_name__{source_id}.tar.gz'
            if found:
                break
        assert found
        # TODO: check that the output is not only present, but also matches expectations
    else:
        print("Not configured to transfer output!")
        assert False
