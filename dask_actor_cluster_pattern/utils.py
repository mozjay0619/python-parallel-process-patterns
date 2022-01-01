import errno
import os
import shutil
import socket
import time
import zipfile
from urllib.parse import urlparse

import boto3
import botocore


def get_host_ip_address():
    """Get the host ip address of the machine where the executor is running.

    Returns
    -------
    host_ip : String
    """
    try:
        host_name = socket.gethostname()
        host_ip = socket.gethostbyname(host_name)
        return host_ip
    except:
        host_ip = "127.0.0.1"  # playing with fire...
        return host_ip


def s3_upload_dir(target_dir, s3_url, object_name):
    """First create a zipped version of the desired directory to upload, and then upload the zipped
    directory into S3. Delete the zipped directory afterwards.

    Parameters
    ----------
    target_dir : String
        The path of the directory to be zipped and uploaded.
    s3_url : String
        The s3 url up to and including the bucket name.
    object_name : String
        The desired name for the S3 key of the uploaded zipped directory file.
    """
    scheme, bucket, path, _, _, _ = urlparse(s3_url)
    key = os.path.join(path[1:], object_name)

    zip_file_dir = shutil.make_archive(target_dir, "zip", target_dir)

    s3_client = boto3.resource("s3").meta.client
    s3_client.upload_file(Filename=zip_file_dir, Bucket=bucket, Key=key)

    os.remove(zip_file_dir)

    return key


import errno
import zipfile


def s3_download_dir(parent_dir, s3_url, key):
    """Download all objects with designated name prefix from S3 bucket into desired destination directory.
    If the destination directory does not exist, create one before downloading the objects into it.

    Parameters
    ----------
    dest_dir : String
            The path to the directory into which the objects are to be stored.
    s3_url : String
            The s3 url up to and including the bucket name.
    object_name_prefix : String
            The prefix of the object name to use to filter the download.
    """
    try:
        os.makedirs(parent_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    filename = os.path.join(parent_dir, key.split("/")[-1])

    scheme, bucket, path, _, _, _ = urlparse(s3_url)

    s3_client = boto3.resource("s3").meta.client
    s3_client.download_file(Filename=filename, Bucket=bucket, Key=key)

    target_dir = filename.replace(".zip", "")
    with zipfile.ZipFile(filename, "r") as f:
        f.extractall(target_dir)

    return target_dir


def s3_delete_object(s3_url, object_name_prefix):
    """Delete all objects with the designated key name prefix from S3 bucket.

    Parameters
    ----------
    s3_url : String
            The s3 url up to and including the bucket name.
    object_name_prefix : String
            The key name prefix to filter the deletion.
    """

    scheme, bucket, path, _, _, _ = urlparse(s3_url)
    prefix = os.path.join(path[1:], object_name_prefix)

    s3_bucket = boto3.resource("s3").Bucket(name=bucket)

    for obj in s3_bucket.objects.filter(Prefix=prefix):

        s3_bucket.Object(key=obj.key).delete()


def s3_object_exists(s3_url, object_name):
    """Return boolean value of True if the object exists in S3, False otherwise.

    Parameters
    ----------
    s3_url : String
            The s3 url up to and including the bucket name.
    object_name : String
            The total and exact name of the key of the object.
    """
    scheme, bucket, path, _, _, _ = urlparse(s3_url)
    key = os.path.join(path[1:], object_name)

    try:
        boto3.resource("s3").Object(bucket_name=bucket, key=key).load()
        return True

    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            return False  # in case we want to handle non 404 differently in the future


class ProgressBar:
    def __init__(self, total):

        self.start_time = time.time()
        self.total = total

    def report(self, iteration):

        if iteration < self.total:
            self.print_progress_bar(iteration, self.total)
        else:
            self.print_progress_bar(self.total, self.total, suffix="Completed!")

    def print_progress_bar(self, iteration, total, suffix="", length=50, fill="█"):

        dur = time.time() - self.start_time
        time_str = time.strftime("%H:%M:%S", time.gmtime(dur))

        percent = ("{0:.1f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + "-" * (length - filledLength)
        print(f"\rProgress: |{bar}| {percent}% | {time_str}  {suffix}", end="\r")
        if iteration == total and suffix == "Completed!":
            print(f"\r", end="\n")
