import threading
import boto3
import os
import sys
import argparse
import getopt
from boto3.s3.transfer import TransferConfig

s3 = boto3.resource('s3')

GB = 1024 ** 3

def multi_part_upload_with_s3():
    # Ensure that multipart uploads only happen if the size of a transfer
    # is larger than S3's size limit for nonmultipart uploads, which is 5 GB.
    config = TransferConfig(multipart_threshold=5 * GB, max_concurrency=10,use_threads=True)

    #config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
    #                        multipart_chunksize=1024 * 25, use_threads=True)

    key_path = 'multipart_files/' + file_name
    s3.meta.client.upload_file(file_name, bucket, dest_file_name,
                            ExtraArgs={},
                            Config=config,
                            Callback=ProgressPercentage(file_name)
                            )


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()


    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


def main(argv):
    global file_name, dest_file_name, bucket

    # commandline argument definitions
    parser = argparse.ArgumentParser(description="S3 Multipart Loader")
    parser.add_argument("-u", "--upload_file", type=str, required=True, help="file to upload")
    parser.add_argument("-d", "--dest_file", type=str, help="destination path/file")
    parser.add_argument("-b", "--bucket", type=str, default="backups-glacier-rsn", help="target bucket name")
    args = parser.parse_args()

    file_name = args.upload_file
    dest_file_name = args.dest_file
    bucket = args.bucket
    if dest_file_name is None:
        dest_file_name = os.path.basename(file_name)
    multi_part_upload_with_s3()


if __name__ == "__main__":
    main(sys.argv[1:])
    
