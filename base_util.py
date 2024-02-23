from typing import Any, List
from yacs.config import CfgNode
import os
from pathlib import Path
import logging

LOG_FORMAT = "%(asctime)s|%(levelname)s|%(process)d|%(module)s|%(funcName)s|%(lineno)d|%(message)s"
logger = logging.getLogger(__name__)


def validate_config(config: CfgNode) -> bool:
    """Check the configuration (supplied by config.yml).

    Return True if config is valid, False otherwise.
    Important note on how DANE builds up it's config:
    FIRST the home dir config is applied (~/.DANE/config.yml),
    THEN the local base_config.yml will overwrite anything specified
    THEN the local config.yml will overwrite anything specified there.
    Also Consult https://github.com/beeldengeluid/dane-example-worker/wiki/Config. 
    Most of the config listed is related to DANE and do not need to be altered when
    developing locally, except the last part (settings for this worker specifically).
    """
    try:
        __validate_environment_variables()
    except AssertionError as e:
        print("Error malconfigured worker: env vars incomplete")
        print(str(e))
        return False

    try:
        # rabbitmq settings
        assert config.RABBITMQ, "RABBITMQ"
        assert check_setting(config.RABBITMQ.HOST, str), "RABBITMQ.HOST"
        assert check_setting(config.RABBITMQ.PORT, int), "RABBITMQ.PORT"
        assert check_setting(config.RABBITMQ.EXCHANGE, str), "RABBITMQ.EXCHANGE"
        assert check_setting(
            config.RABBITMQ.RESPONSE_QUEUE, str
        ), "RABBITMQ.RESPONSE_QUEUE"
        assert check_setting(config.RABBITMQ.USER, str), "RABBITMQ.USER"
        assert check_setting(config.RABBITMQ.PASSWORD, str), "RABBITMQ.PASSWORD"

        # Elasticsearch settings
        assert config.ELASTICSEARCH, "ELASTICSEARCH"
        assert check_setting(config.ELASTICSEARCH.HOST, list), "ELASTICSEARCH.HOST"
        assert (
            len(config.ELASTICSEARCH.HOST) == 1
            and type(config.ELASTICSEARCH.HOST[0]) is str
        ), "Invalid ELASTICSEARCH.HOST"

        assert check_setting(config.ELASTICSEARCH.PORT, int), "ELASTICSEARCH.PORT"
        assert check_setting(config.ELASTICSEARCH.USER, str, True), "ELASTICSEARCH.USER"
        assert check_setting(
            config.ELASTICSEARCH.PASSWORD, str, True
        ), "ELASTICSEARCH.PASSWORD"
        assert check_setting(config.ELASTICSEARCH.SCHEME, str), "ELASTICSEARCH.SCHEME"
        assert check_setting(config.ELASTICSEARCH.INDEX, str), "ELASTICSEARCH.INDEX"

        # DANE python lib settings
        assert config.PATHS, "PATHS"
        assert check_setting(config.PATHS.TEMP_FOLDER, str), "PATHS.TEMP_FOLDER"
        assert check_setting(config.PATHS.OUT_FOLDER, str), "PATHS.OUT_FOLDER"

        assert config.FILE_SYSTEM, "FILE_SYSTEM"
        assert check_setting(
            config.FILE_SYSTEM.BASE_MOUNT, str
        ), "FILE_SYSTEM.BASE_MOUNT"
        assert check_setting(config.FILE_SYSTEM.INPUT_DIR, str), "FILE_SYSTEM.INPUT_DIR"
        assert check_setting(
            config.FILE_SYSTEM.OUTPUT_DIR, str
        ), "FILE_SYSTEM.OUTPUT_DIR"

        # settings for this worker specifically
        # TODO: check all relevant settings

        # settings for input & output handling
        assert config.INPUT, "INPUT"
        assert check_setting(
            config.INPUT.S3_ENDPOINT_URL, str, True
        ), "INPUT.S3_ENDPOINT_URL"
        assert check_setting(
            config.INPUT.MODEL_CHECKPOINT_S3_URI, str, True
        ), "INPUT.MODEL_CHECKPOINT_S3_URI"
        assert check_setting(
            config.INPUT.MODEL_CONFIG_S3_URI, str, True
        ), "INPUT.MODEL_CONFIG_S3_URI"
        assert check_setting(
            config.INPUT.DELETE_ON_COMPLETION, bool
        ), "INPUT.DELETE_ON_COMPLETION"

        assert config.OUTPUT, "OUTPUT"
        assert check_setting(
            config.OUTPUT.DELETE_ON_COMPLETION, bool
        ), "OUTPUT.DELETE_ON_COMPLETION"
        assert check_setting(
            config.OUTPUT.TRANSFER_ON_COMPLETION, bool
        ), "OUTPUT.TRANSFER_ON_COMPLETION"
        if config.OUTPUT.TRANSFER_ON_COMPLETION:
            # required only in case output must be transferred
            assert check_setting(
                config.OUTPUT.S3_ENDPOINT_URL, str
            ), "OUTPUT.S3_ENDPOINT_URL"
            assert check_setting(config.OUTPUT.S3_BUCKET, str), "OUTPUT.S3_BUCKET"
            assert check_setting(
                config.OUTPUT.S3_FOLDER_IN_BUCKET, str
            ), "OUTPUT.S3_FOLDER_IN_BUCKET"

        assert __check_dane_dependencies(config.DANE_DEPENDENCIES), "DANE_DEPENDENCIES"

    except AssertionError as e:
        print(f"Configuration error: {str(e)}")
        return False

    return True


def __validate_environment_variables() -> None:
    # self.UNIT_TESTING = os.getenv('DW_ASR_UNIT_TESTING', False)
    try:
        assert True  # TODO add secrets from the config.yml to the env
    except AssertionError as e:
        raise (e)


def check_setting(setting: Any, t: type, optional=False) -> bool:
    return (type(setting) is t and optional is False) or (
        optional and (setting is None or type(setting) is t)
    )


def __check_dane_dependencies(deps: Any) -> bool:
    """The idea is that you specify a bit more strictly that your worker can only
    work on the OUTPUT of another worker.
    If you want to define a dependency, you should populate the deps_allowed list 
    in this function with valid keys, that other workers use to identify themselves
    within DANE: just use the queue_name
    (see e.g. https://github.com/beeldengeluid/dane-video-segmentation-worker/blob/main/worker.py#L34-L35)
    Then also make sure you define a valid dependency in your worker here: 
    https://github.com/beeldengeluid/dane-video-segmentation-worker/blob/main/worker.py#L36-L38 
    (using another worker as an example)
    """
    deps_to_check: list = deps if type(deps) is list else []
    deps_allowed: list = []
    return all(dep in deps_allowed for dep in deps_to_check)


def __validate_parent_dirs(paths: list) -> None:
    try:
        for p in paths:
            assert os.path.exists(
                Path(p).parent.absolute()
            ), f"Parent dir of file does not exist: {p}"
    except AssertionError as e:
        raise (e)
