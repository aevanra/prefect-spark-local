from pyspark.sql import SparkSession

def get_aws_authenticated_session(session_name: str) -> SparkSession:
    """ Utility to get AWS authenticated SparkSession object """
    return (
        SparkSession
            .builder
            .appName(session_name)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
            .config("spark.jars.packages", ",".join([
                "org.apache.hadoop:hadoop-aws:3.4.1",
                "com.amazonaws:aws-java-sdk-bundle:1.12.787"
            ]))
            .getOrCreate()
    )
