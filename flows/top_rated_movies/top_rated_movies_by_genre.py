import s3fs
import contextlib
import shutil
import subprocess
import threading
from prefect import flow, task
from prefect.logging import  get_run_logger
from prefect.deployments.runner import DockerImage
from src.constants import DATA_DIR, WORK_DIR, EXPORT_DIR
from src.io import move_file_to_output_on_aws
from src.exceptions import UpstreamFailedException
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
def run_spark_job(task_dependency: str) -> None:
    if not task_dependency:
        raise UpstreamFailedException("Task dependency failed")


    logger = get_run_logger()
    command = [
        "spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.787",
        "--py-files", "./src.zip",
        "--conf", "spark.executor.memory=2g",
        "--conf", "spark.executor.cores=2",
        "--conf", "spark.executor.instances=3",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com",
        "--conf", "spark.hadoop.fs.s3a.connection.timeout=60000",
        "--conf", "spark.hadoop.fs.s3a.connection.establish.timeout=5000",
        "--conf", "spark.hadoop.fs.s3a.attempts.maximum=5",
        "--conf", "spark.hadoop.fs.s3a.readahead.range=65536",
        "--conf", "spark.hadoop.fs.s3a.threads.keepalivetime=60000",
        "--conf", "spark.hadoop.fs.s3a.multipart.purge.age=86400000",
        "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "./spark_job/main.py", "50", "4"
    ]

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

    def log_stream(stream, log_func):
        for line in iter(stream.readline, ''):
            log_func(line.rstrip())

    stdout_thread = threading.Thread(target=log_stream, args=(process.stdout, logger.info))
    stderr_thread = threading.Thread(target=log_stream, args=(process.stderr, logger.error))

    stdout_thread.start()
    stderr_thread.start()

    stdout_thread.join()
    stderr_thread.join()

    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)

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
    path = run_spark_job(t1)
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
        shutil.rmtree
