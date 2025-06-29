import logging 
import s3fs

from .exceptions import NoFileException


def move_file_to_output_on_aws(
    source_path: str,
    dest_path: str,
    fs: s3fs.S3FileSystem | None = s3fs.S3FileSystem(anon=False),
    logger: logging.Logger | None = logging.getLogger()
) -> str:
    """Utility function to move a spark part- file to final output location and name"""
    if not fs:
        fs = s3fs.S3FileSystem(anon=False)

    csv_files = [
        file
        for file in fs.ls(source_path)
        if file.endswith(".csv")
    ]
    logger.info(f"Found {len(csv_files)} files in {source_path}")

    if not csv_files:
        raise NoFileException

    return consolidate_csvs(csv_files, dest_path, fs, logger)
        



def consolidate_csvs(input_files: list[str], output_filepath: str, fs: s3fs.S3FileSystem | None = s3fs.S3FileSystem(anon=False), logger: logging.Logger | None = logging.getLogger()
) -> str:
    """Consolidate list of CSV files into one file"""
    with fs.open(output_filepath, "wb") as output:
        first_file = True
        for file in input_files:
            file = f"s3://{file}"
            logger.info(f"Reading {file}")
            with fs.open(file, "rb") as input:
                lines = input.read().decode("utf-8").splitlines()
                if not lines:
                    continue  # skip empty files
                if first_file:
                    # Write header and all rows
                    output.write(("\n".join(lines) + "\n").encode("utf-8"))
                    first_file = False
                else:
                    # Skip header, write rest
                    output.write(("\n".join(lines[1:]) + "\n").encode("utf-8"))
    return output_filepath

