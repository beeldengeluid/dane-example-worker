import logging
import os
from pathlib import Path
import shutil
import tarfile
from time import time
from typing import Dict, List

from dane import Document
from dane.config import cfg
from dane.s3_util import S3Store, parse_s3_uri, validate_s3_uri
from models import (
    OutputType,
    Provenance,
    ThisWorkerInput,
)


logger = logging.getLogger(__name__)
INPUT_GENERATOR_TASK_KEY = "SOME_KEY"
OUTPUT_FILE_BASE_NAME = "base_name"
TAR_GZ_EXTENSION = ".tar.gz"
# specify output types to upload to S3
S3_OUTPUT_TYPES: List[OutputType] = [
    # TODO: add any output types
    OutputType.PROVENANCE,
]


def validate_data_dirs() -> bool:
    """Make sure the necessary base dirs are there."""
    dirs = {
        "input": Path(get_download_dir()),
        "output": Path(get_base_output_dir())
        # TODO: perhaps add model dir
    }
    base = dirs["input"].parent.absolute()
    if not os.path.exists(base):
        logger.info(
            f"{base} does not exist." "Make sure BASE_MOUNT_DIR exists before retrying"
        )
        return False

    for kind, dir in dirs.items():
        try:
            os.makedirs(dir, 0o755)
            logger.info(f"created {kind} dir: {dir}.")
        except FileExistsError as e:
            logger.info(e)
    return True


def generate_output_dirs(source_id: str) -> Dict[str, str]:
    """For each OutputType, create a subdir inside the base output dir"""
    base_output_dir = get_base_output_dir(source_id)
    output_dirs = {}
    for output_type in OutputType:
        output_dir = os.path.join(base_output_dir, output_type.value)
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)
        output_dirs[output_type.value] = output_dir
    return output_dirs


def get_base_output_dir(source_id: str = "") -> str:
    """Return path of which processing modules write output data in a subfolder"""
    path_elements = [cfg.FILE_SYSTEM.BASE_MOUNT, cfg.FILE_SYSTEM.OUTPUT_DIR]
    if source_id:
        path_elements.append(source_id)
    return os.path.join(*path_elements)


def get_archive_file_path(source_id: str) -> str:
    """Return file name of the final tar.gz that will be uploaded to S3"""
    return os.path.join(
        get_base_output_dir(source_id),
        f"{OUTPUT_FILE_BASE_NAME}__{source_id}{TAR_GZ_EXTENSION}",
    )


def get_output_file_name(source_id: str, output_type: OutputType) -> str:
    """Return file name for specified OutputType"""
    # TODO: specify output file name based on source_id and output_type
    match output_type:
        # TODO: add cases for other output
        case OutputType.PROVENANCE:
            output_file_name = "provenance.json"
        case OutputType.FOOBAR:
            output_file_name = f"{source_id}_foobar.txt"
        case _:
            output_file_name = ""
    return output_file_name


def get_output_file_path(source_id: str, output_type: OutputType) -> str:
    """Return file path for specified OutputType"""
    return os.path.join(
        get_base_output_dir(source_id),
        output_type.value,
        get_output_file_name(source_id, output_type),
    )


def get_s3_base_uri(source_id: str) -> str:
    """Return base uri for configured S3 folder.

    e.g. s3://<bucket>/assets/<source_id>"""
    uri = os.path.join(cfg.OUTPUT.S3_BUCKET, cfg.OUTPUT.S3_FOLDER_IN_BUCKET, source_id)
    return f"s3://{uri}"


def get_s3_output_file_uri(source_id: str) -> str:
    """Return entire output uri for configured S3 folder.

    e.g. s3://<bucket>/assets/<source_id>/<basename>__<source_id>.tar.gz
    """
    return f"{get_s3_base_uri(source_id)}/{get_archive_file_path(source_id)}"


def get_source_id_from_tar(input_path: str) -> str:
    """Parse filename and return source_id.

    NOTE: only use for test run & unit test with input that points to tar file!
    e.g. ./data/input-files/<basename>__testob.tar.gz"""
    fn = os.path.basename(input_path)
    tmp = fn.split("__")
    source_id = tmp[1][: -len(TAR_GZ_EXTENSION)]
    logger.info(f"Using source_id: {source_id}")
    return source_id


def source_id_from_s3_uri(s3_uri: str) -> str:
    """Parse s3_uri and return source_id.

    e.g. s3://<bucket>/assets/<source_id>/<basename>__<source_id>.tar.gz
    """
    fn = os.path.basename(s3_uri)
    fn = fn.replace(".tar.gz", "")
    source_id = "__".join(fn.split("__")[1:])
    return source_id


def delete_local_output(source_id: str) -> bool:
    """Delete anything written to local filesystem.

    To be used as cleanup, after archiving and uploading to S3.
    """
    output_dir = get_base_output_dir(source_id)
    logger.info(f"Deleting output folder: {output_dir}")
    if output_dir == os.sep or output_dir == ".":
        logger.warning(f"Rejected deletion of: {output_dir}")
        return False

    if not _is_valid_output(output_dir):
        logger.warning(
            f"Tried to delete a dir that did not contain output: {output_dir}"
        )
        return False

    try:
        shutil.rmtree(output_dir)
        logger.info(f"Cleaned up folder {output_dir}")
    except Exception:
        logger.exception(f"Failed to delete output dir {output_dir}")
        return False
    return True


def _is_valid_output(output_dir: str) -> bool:
    """Assert all relevant output subdirectories exist."""
    # TODO implement some more, now checks presence of provenance dir
    to_check = [OutputType.PROVENANCE]
    valid = [
        os.path.exists(os.path.join(output_dir, outputtype.value))
        for outputtype in to_check
    ]
    return all(valid)


def _validate_transfer_config() -> bool:
    """Assert that all S3 settings for transfer are in place."""
    if any(
        [
            not x
            for x in [
                cfg.OUTPUT.S3_ENDPOINT_URL,
                cfg.OUTPUT.S3_BUCKET,
                cfg.OUTPUT.S3_FOLDER_IN_BUCKET,
            ]
        ]
    ):
        logger.warning(
            "TRANSFER_ON_COMPLETION configured without all the necessary S3 settings"
        )
        return False
    return True


def transfer_output(source_id: str) -> bool:
    """compress all desired output dirs into a single tar and upload it to S3"""
    output_dir = get_base_output_dir(source_id)
    logger.info(f"Transferring {output_dir} to S3 (asset={source_id})")
    if not _validate_transfer_config():
        return False

    s3 = S3Store(cfg.OUTPUT.S3_ENDPOINT_URL)
    file_list = [os.path.join(output_dir, ot.value) for ot in S3_OUTPUT_TYPES]
    tar_file = get_archive_file_path(source_id)

    success = s3.transfer_to_s3(
        cfg.OUTPUT.S3_BUCKET,
        os.path.join(
            cfg.OUTPUT.S3_FOLDER_IN_BUCKET, source_id
        ),  # assets/<program ID>__<carrier ID>
        file_list,  # this list of subdirs will be compressed into the tar below
        tar_file,  # this file will be uploaded
    )
    if not success:
        logger.error(f"Failed to upload: {tar_file}")
        return False
    return True


def get_download_dir() -> str:
    """Return general location where input should be downloaded in."""
    return os.path.join(cfg.FILE_SYSTEM.BASE_MOUNT, cfg.FILE_SYSTEM.INPUT_DIR)


def get_base_input_dir(source_id: str) -> str:
    """Return location where input should be downloaded in for specified source_id."""
    return os.path.join(get_download_dir(), source_id)


def delete_input_file(input_file: str, source_id: str, actually_delete: bool) -> bool:
    """Delete specified input archive + corresponding file structure, report success"""
    # TODO: refactor
    logger.info(f"Verifying deletion of input file: {input_file}")
    if actually_delete is False:
        logger.info("Configured to leave the input alone, skipping deletion")
        return True

    # first remove the input file
    try:
        os.remove(input_file)
        logger.info(f"Deleted input tar file: {input_file}")
    except OSError:
        logger.exception("Could not delete input file")
        return False

    # now remove the folders that were extracted from the input tar file
    base_input_dir = get_base_input_dir(source_id)
    try:
        for root, dirs, files in os.walk(base_input_dir):
            for d in dirs:
                dir_path = os.path.join(root, d)
                logger.info(f"Deleting {dir_path}")
                shutil.rmtree(dir_path)
        logger.info("Deleted extracted input dirs")
        os.removedirs(base_input_dir)
        logger.info(f"Finally deleted the base_input_dir: {base_input_dir}")
    except OSError:
        logger.exception("OSError while removing empty input file dirs")
    except FileNotFoundError:
        logger.exception("FileNotFoundError while removing empty input file dirs")

    return True  # return True even if empty dirs were not removed


def obtain_input_file(s3_uri: str) -> ThisWorkerInput:
    """Obtain input from s3_uri, report in the form of ThisWorkerInput"""

    if not validate_s3_uri(s3_uri):
        return ThisWorkerInput(500, f"Invalid S3 URI: {s3_uri}")

    source_id = source_id_from_s3_uri(s3_uri)
    start_time = time()
    output_folder = get_base_input_dir(source_id)

    # TODO download the content into get_download_dir()
    s3 = S3Store(cfg.OUTPUT.S3_ENDPOINT_URL)
    bucket, object_name = parse_s3_uri(s3_uri)
    logger.info(f"OBJECT NAME: {object_name}")
    input_file_path = os.path.join(
        get_download_dir(),
        source_id,
        os.path.basename(object_name),  # i.e. <input_base>__<source_id>.tar.gz
    )
    success = s3.download_file(bucket, object_name, output_folder)
    if success:
        # TODO uncompress the <input_base>.tar.gz

        provenance = Provenance(
            activity_name="download",
            activity_description="Download input data",
            start_time_unix=start_time,
            processing_time_ms=time() - start_time,
            input_data={},
            output_data={"file_path": input_file_path},
        )
        return ThisWorkerInput(
            200,
            f"Downloaded input from: {s3_uri}",
            source_id_from_s3_uri(s3_uri),  # source_id
            input_file_path,  # locally downloaded .tar.gz
            provenance,
        )
    logger.error("Failed to download input data from S3")
    return ThisWorkerInput(500, f"Failed to download: {s3_uri}")


def fetch_input_s3_uri(handler, doc: Document) -> str:
    logger.info("checking input")
    possibles = handler.searchResult(doc._id, INPUT_GENERATOR_TASK_KEY)
    logger.info(possibles)
    if len(possibles) > 0 and "s3_location" in possibles[0].payload:
        return possibles[0].payload.get("s3_location", "")
    logger.error(f"No s3_location found in result for {INPUT_GENERATOR_TASK_KEY}")
    return ""


def untar_input_file(tar_file_path: str):
    """Untar archive (.tar.gz) into the same dir"""
    # TODO: explicitly report back?
    logger.info(f"Uncompressing {tar_file_path}")
    path = str(Path(tar_file_path).parent)
    with tarfile.open(tar_file_path) as tar:
        tar.extractall(path=path, filter="data")  # type: ignore
