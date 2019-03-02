import boto3
import os
import sys
from boto3.s3.transfer import TransferConfig

# Get the service client
s3 = boto3.client('s3')


if len(sys.argv) < 2:
    print("Usage: sys.argv[0] <upload_file> [<bucket_name>]")
    exit(2)

file_name = sys.argv[1]
dest_file_name = os.path.basename(file_name)

if len(sys.argv) > 2:
    bucket = sys.argv[2]
elif os.getenv('S3_BUCKET_NAME') is not None:
    bucket = os.getenv('S3_BUCKET_NAME')
else:
    bucket = "backups-glacier-rsn"

GB = 1024 ** 3

# Ensure that multipart uploads only happen if the size of a transfer
# is larger than S3's size limit for nonmultipart uploads, which is 5 GB.
config = TransferConfig(multipart_threshold=5 * GB)

s3.upload_file(file_name, bucket, dest_file_name, Config=config)

