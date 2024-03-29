from moto import mock_aws
import boto3
import pytest
import os, shutil
import tarfile

from main_data_processor import run
from dane.config import cfg
from io_util import untar_input_file, S3_OUTPUT_TYPES


source_id = "resource__carrier"
fn_tar_in = f"prep__{source_id}.tar.gz"
key_in = f"{cfg.INPUT.S3_FOLDER_IN_BUCKET}/{fn_tar_in}"
tar_out = f"{source_id}/base_name__{source_id}.tar.gz"
key_out = f"{cfg.OUTPUT.S3_FOLDER_IN_BUCKET}/{tar_out}"


@pytest.fixture
def aws_credentials():
    """Create custom AWS setup: mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"  # Other regions make stuff complex
    os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = cfg.INPUT.S3_ENDPOINT_URL


@pytest.fixture
def aws(aws_credentials):
    """Spin up local aws for testing"""
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture
def create_sample_input():
    """Add sample input for test to input bucket.
    In this case, sample input is created on the fly. 
    It is also possible to download a file here (e.g. from Openbeelden), 
    or add a file from the repository (e.g. from data/input-files/<example-input>)"""
    fn = f'{source_id}.input'
    with open(fn, 'w') as f:
        f.write('This is just a file with some random input')
    with tarfile.open(fn_tar_in, 'w:gz') as tar:
        tar.add(fn)
    yield
    # after test: cleanup
    os.remove(fn)
    os.remove(fn_tar_in)
    

@pytest.fixture
def create_and_fill_buckets(aws, create_sample_input):
    """Make sure input and output buckets exist, and add sample input"""
    client = boto3.client("s3")
    for bucket in [
        cfg.INPUT.S3_BUCKET,
        cfg.OUTPUT.S3_BUCKET,
        cfg.INPUT.S3_BUCKET_MODEL
    ]:
        client.create_bucket(Bucket=bucket)
    client.put_object(Body=fn_tar_in,  # "tests/integration/resource__carrier.input",
                      Bucket=cfg.INPUT.S3_BUCKET,
                      Key=f"{cfg.INPUT.S3_FOLDER_IN_BUCKET}/{key_in}")


@pytest.fixture
def setup_fs():
    try:
        os.makedirs(source_id)
    except FileExistsError:
        print("Destination for output is not empty: abort.")
        assert False
    yield
    # after test: cleanup
    # os.remove(tar_out)
    shutil.rmtree(source_id)


def test_main_data_processor(aws, aws_credentials, create_and_fill_buckets, setup_fs):
    """Test the main_data_processor.run function, running on URI in mocked S3.
    Relies on fixtures: aws, aws_credentials, create_and_fill_buckets, setup_fs"""
    if cfg.OUTPUT.TRANSFER_ON_COMPLETION:
        # run the main data processor
        run(input_file_path=f"s3://{cfg.INPUT.S3_BUCKET}/{cfg.INPUT.S3_FOLDER_IN_BUCKET}/{key_in}")
        
        # Check if the output is present in S3
        client = boto3.client("s3")
        found = False
        for item in client.list_objects(Bucket=cfg.OUTPUT.S3_BUCKET)['Contents']:
            found = item['Key'] == key_out
            if found:
                break
        assert found
        
        # TODO: check that the output matches expectations
        client.download_file(Bucket=cfg.OUTPUT.S3_BUCKET, Key=key_out, Filename=tar_out)
        untar_input_file(tar_out)
        for type in S3_OUTPUT_TYPES:
            assert type.value in os.listdir(source_id)
            # TODO: further checking of the output (optional)

    else:
        print("Not configured to transfer output!")
        assert False
