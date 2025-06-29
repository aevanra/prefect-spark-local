import logging
import subprocess
import threading

def run_spark_job(
        job_path: str,
        head_uri: str | None = "spark://spark-master:7077",
        job_memory: str = "2g",
        num_cores: int = 2,
        num_instances: int = 3,
        logger: logging.Logger = logging.getLogger(),
        job_args: list[str] | None = None,
        ) -> None:
    command = [
        "spark-submit",
        "--master", head_uri,
        "--deploy-mode", "client",
        "--packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.787",
        "--py-files", "./src.zip",
        "--conf", f"spark.executor.memory={job_memory}",
        "--conf", f"spark.executor.cores={num_cores}",
        "--conf", f"spark.executor.instances={num_instances}",
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
        job_path, *job_args
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
