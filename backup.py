import os
from hashlib import sha3_256

import boto3
from botocore.exceptions import ClientError

# GLOBAL INFO
aws = {"aws_access_key_id": "", "aws_secret_access_key": "", "aws_session_token": ""}


def get_hash(path):
    with open(path, "r") as f:
        text = f.readlines()[0]
        engine = sha3_256()
        engine.update(text.encode('UTF8'))
        return engine.hexdigest()


def get_info(path):
    """
    get the info to access the s3 service
    :param path: name of the folder which contains all the needed file to read
    :return:
    """
    for key, value in aws.items():
        with open(path + "/" + key + ".txt", "r") as f:
            aws[key] = f.readlines()[0]


if __name__ == '__main__':

    # get info to start the session
    get_info("keys")

    # get bucket name
    with open("keys/bucketname.txt", "r") as f:
        bucket_name = f.readlines()[0]

    # start the session
    session = boto3.Session(
        aws_access_key_id=aws["aws_access_key_id"],
        aws_secret_access_key=aws["aws_secret_access_key"],
        aws_session_token=aws["aws_session_token"]
    )

    s3 = session.resource('s3')

    bucket = s3.Bucket(bucket_name)

    # print(bucket_name)

    # for s3_file in bucket.objects.all():
        # print(s3_file.key)

    file_to_upload = []
    files_dir = "files"

    directory = os.fsencode(files_dir)

    for file in os.listdir(directory):
        upload = False
        filename = os.fsdecode(file)

        # bucket.upload_file("files/"+filename, filename)

        print('Downloading ' + filename)

        try:
            bucket.download_file(filename, "tmp/"+filename)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                upload = True
                file_to_upload.append(filename)
            else:
                raise
        if not upload:
            hash1 = get_hash(files_dir+"/"+filename)
            hash2 = get_hash("tmp/"+filename)

            print(hash1)
            print(hash2)

            if hash1 != hash2:
                upload = True
                file_to_upload.append(filename)

        print("File: " + filename)
        print("Updated: " + str(upload))

    for file in file_to_upload:
        print('Uploading ' + file)
        try:
            bucket.upload_file(files_dir + "/" + file, file)
        except Exception as e:
            print('Couldn\'t upload ' + file)
            print(e)
