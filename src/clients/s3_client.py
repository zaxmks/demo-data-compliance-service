import os
from os import environ
from boto3 import resource
import tempfile


class S3Client(object):
    def __init__(self, bucket=None):
        self.bucket_name = bucket if bucket else environ.get("AWS_S3_BUCKET_NAME")
        self.s3 = resource(
            "s3",
        )
        self.temporary_files = []

    def resource(self):
        return self.s3

    def get_file(self, key):
        """Returns file in memory"""
        return self.s3.Object(self.bucket_name, key).get()["Body"].read()

    def download_file(self, key, local_file_path):
        """Downloads file at "key" to local file system"""
        self.s3.Bucket(self.bucket_name).download_file(key, local_file_path)

    def download_tmp_file(self, key, prefix=None):
        """Downloads the file into a temp file that is erased upon deletion of this client object."""
        self.temporary_files.append(
            tempfile.NamedTemporaryFile(
                mode="w+b", prefix=prefix, suffix="_" + os.path.basename(key)
            )
        )
        self.temporary_files[-1].write(self.get_file(key))
        self.temporary_files[-1].flush()
        return self.temporary_files[-1].name

    def put_file(self, key, body):
        """Puts a file in memory onto the s3 bucket"""
        self.s3.Object(self.bucket_name, key).put(Body=body)

    def upload_file(self, key, file_name):
        """Uploads a file from local file system to s3 bucket"""
        self.s3.Bucket(self.bucket_name).upload_file(file_name, key)

    def delete_file(self, key):
        self.s3.Object(self.bucket_name, key).delete()

    def list_files(self, filter=None, limit=None):
        objects = self.s3.Bucket(self.bucket_name).objects
        objects = objects.filter(Prefix=filter) if filter else objects
        objects = objects.limit(limit) if limit else objects
        objects = objects.all() if limit is None and filter is None else objects
        return [object.key for object in objects]

    def move_file(self, current_key, new_key):
        # Not used
        self.s3.Object(self.bucket_name, new_key).copy_from(
            CopySource={"Bucket": self.bucket_name, "Key": current_key}
        )
        self.s3.Object(self.bucket_name, current_key).delete()
