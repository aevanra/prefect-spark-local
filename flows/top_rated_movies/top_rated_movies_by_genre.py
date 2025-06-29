import s3fs
import contextlib
import shutil
from prefect import flow, task
from prefect.logging import  get_run_logger
from prefect.deployments.runner import DockerImage
from src.constants import DATA_DIR, WORK_DIR, EXPORT_DIR
from src.io import move_file_to_output_on_aws
from src.exceptions import UpstreamFailedException
from src.spark_runner import run_spark_job
from pathlib import Path

PROCESS_NAME = "top_movies_by_genre"

fs = s3fs.S3FileSystem(anon=False)

@task
def upload_data() -> None:
    """ Check that files exist in S3 and upload if not """
    logger = get_run_logger()
    existing_files = fs.ls(DATA_DIR)
    existing_files = {Path(f).name for f in existing_files}
    logger.info(f"Found: {existing_files}")

    ftypes = ["u.data", "u.item", "u.user"]

    ftypes = [ftype for ftype in ftypes if ftype not in existing_files]

    if ftypes:
        for ftype in ftypes:
            logger.info(f"Uploading {ftype}")
            fs.put(f"./data/{ftype}", f"{DATA_DIR}/{ftype}")
            logger.info(f"Uploaded {ftype} to {DATA_DIR}/{ftype}")

    return "done"


@task
def spark_job(task_dependency: str) -> None:
    if not task_dependency:
        raise UpstreamFailedException("Task dependency failed")
    
    logger = get_run_logger()

    run_spark_job(
            job_path="./spark_job/main.py",
            logger=logger,
            job_args=[
                "50", # Minimum ratings to count per movie
                "4" # decimal precision
                ],
            )

    # Return working directory for spark
    return f"{WORK_DIR}/top_movies_by_genre"
    
@task
def consolidate_spark_output(dir: str) -> None:
    """ Move spark part files to a single CSV in output location """
    logger = get_run_logger()
    move_file_to_output_on_aws(source_path = dir, dest_path=EXPORT_DIR, fs=fs, logger=logger)

@flow
def pipeline() -> None:
    t1 = upload_data()
    path = spark_job(t1)
    consolidate_spark_output(dir=path)


if __name__ == "__main__":
    with contextlib.suppress(FileExistsError):
        shutil.copytree("../../src/", "./src", True)
    with contextlib.suppress(FileExistsError):
        shutil.copytree("../../data/", "./data", True)

    try:
        pipeline.deploy(
                name=PROCESS_NAME,
                work_pool_name="default",
                image=DockerImage(
                    name=f"aevanra/{PROCESS_NAME}",
                    dockerfile="Dockerfile",
                    tag="latest",
                ),
                build=True,
                push=True,
            )

    finally:
        shutil.rmtree("./src")
        shutil.rmtree("./data")
