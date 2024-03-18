from moto import mock_aws
from worker import ExampleWorker
from base_util import LOG_FORMAT
import logging
import sys
from dane.config import cfg
import pytest

#@mock_aws
def test_run_worker():

    logging.basicConfig(
        stream=sys.stdout,  # configure a stream handler only for now (single handler)
        format=LOG_FORMAT,
    )
    worker = ExampleWorker(cfg)
    assert True
    # worker.run()
