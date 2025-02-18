#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3",
# ]
# ///

import argparse
import json
import random
import string
import subprocess
from enum import Enum
from pathlib import Path

import boto3

REGION_NAME = "us-east-1"


class Actions(str, Enum):
    Get = "Get"
    Put = "Put"
    List = "List"


def get_user_dir(bucket_name, user, with_bucket=True):
    if with_bucket:
        return f"{bucket_name}/user-id-{user}"

    return f"user-id-{user}"


def create_power_user():
    power_user_key = "power_user_key"
    power_user_secret = "power_user_secret"
    command = f"s3.configure -apply -user poweruser -access_key {power_user_key} -secret_key {power_user_secret} -actions Admin"
    print("Creating Power User...")
    subprocess.run(
        ["docker", "exec", "-i", "seaweedfs-master-1", "weed", "shell"],
        input=command,
        text=True,
        stdout=subprocess.PIPE,
    )
    print(
        f"Power User created with key: {power_user_key} and secret: {power_user_secret}"
    )
    return power_user_key, power_user_secret


def create_bucket(s3_client, bucket_name):
    print(f"Creating Bucket {bucket_name}...")
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} created.")


def upload_file(s3_client, bucket_name, user, file_path, custom_remote_path=None):
    user_dir = get_user_dir(bucket_name, user, with_bucket=False)
    if custom_remote_path:
        remote_path = custom_remote_path
    else:
        remote_path = f"{user_dir}/{str(Path(file_path).name)}"

    print(f"Uploading {file_path} for {user}... on {user_dir}")

    s3_client.upload_file(file_path, bucket_name, remote_path)
    print(f"File {file_path} uploaded for {user}.")


def create_user(iam_client, user):
    print(f"Creating user {user}...")
    response = iam_client.create_access_key(UserName=user)
    print(
        f"User {user} created with access key: {response['AccessKey']['AccessKeyId']}"
    )
    return response


def list_files(s3_client, bucket_name, path=None):
    if path is None:
        path = ""
    print(f"Listing files of s3://{bucket_name}/{path}...")
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path)
        if "Contents" in response:
            for obj in response["Contents"]:
                print(f"\t - {obj['Key']}")
        else:
            print("No files found.")
    except Exception as e:
        print(f"Error listing files: {e}")


def create_policy_for_user(
    iam_client, user, bucket_name, actions=[Actions.Get, Actions.List]
):
    print(f"Creating policy for {user} on {bucket_name}...")
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [f"s3:{action.value}*" for action in actions],
                "Resource": [
                    f"arn:aws:s3:::{get_user_dir(bucket_name, user)}/*",
                ],
            }
        ],
    }
    policy_name = f"{user}-{bucket_name}-full-access"

    policy_json = json.dumps(policy_document)
    filepath = f"/tmp/{policy_name}.json"
    with open(filepath, "w") as f:
        f.write(json.dumps(policy_document, indent=2))

    iam_client.put_user_policy(
        PolicyName=policy_name, PolicyDocument=policy_json, UserName=user
    )
    print(f"Policy for {user} on {bucket_name} created.")


def main():
    parser = argparse.ArgumentParser(description="SeaweedFS S3 Test Script")
    parser.add_argument(
        "--s3-url", default="http://127.0.0.1:8333", help="S3 endpoint URL"
    )
    parser.add_argument(
        "--iam-url", default="http://127.0.0.1:8111", help="IAM endpoint URL"
    )
    args = parser.parse_args()

    bucket_name = (
        f"test-bucket-{''.join(random.choices(string.digits + 'abcdef', k=8))}"
    )
    sentinel_file = "/tmp/SENTINEL"
    with open(sentinel_file, "w") as f:
        f.write("Hello World")
    print(f"SENTINEL file created at {sentinel_file}")

    power_user_key, power_user_secret = create_power_user()

    admin_s3_client = get_s3_client(args, power_user_key, power_user_secret)
    iam_client = get_iam_client(args, power_user_key, power_user_secret)

    create_bucket(admin_s3_client, bucket_name)
    upload_file(admin_s3_client, bucket_name, "Alice", sentinel_file)
    upload_file(admin_s3_client, bucket_name, "Bob", sentinel_file)
    list_files(admin_s3_client, bucket_name)

    alice_user_info = create_user(iam_client, "Alice")
    bob_user_info = create_user(iam_client, "Bob")

    alice_key = alice_user_info["AccessKey"]["AccessKeyId"]
    alice_secret = alice_user_info["AccessKey"]["SecretAccessKey"]
    bob_key = bob_user_info["AccessKey"]["AccessKeyId"]
    bob_secret = bob_user_info["AccessKey"]["SecretAccessKey"]

    # Make sure Admin can read any files
    list_files(admin_s3_client, bucket_name)
    list_files(
        admin_s3_client,
        bucket_name,
        get_user_dir(bucket_name, "Alice", with_bucket=False),
    )
    list_files(
        admin_s3_client,
        bucket_name,
        get_user_dir(bucket_name, "Bob", with_bucket=False),
    )

    # Create read policy for Alice and Bob
    create_policy_for_user(iam_client, "Alice", bucket_name)
    create_policy_for_user(iam_client, "Bob", bucket_name)

    alice_s3_client = get_s3_client(args, alice_key, alice_secret)

    # Make sure Alice can read her files
    list_files(
        alice_s3_client,
        bucket_name,
        get_user_dir(bucket_name, "Alice", with_bucket=False) + "/",
    )

    # Make sure Bob can read his files
    bob_s3_client = get_s3_client(args, bob_key, bob_secret)
    list_files(
        bob_s3_client,
        bucket_name,
        get_user_dir(bucket_name, "Bob", with_bucket=False) + "/",
    )

    # Update policy to include write
    create_policy_for_user(iam_client, "Alice", bucket_name, actions=[Actions.Put, Actions.Get, Actions.List])  # fmt: off
    create_policy_for_user(iam_client, "Bob", bucket_name, actions=[Actions.Put, Actions.Get, Actions.List])  # fmt: off

    print("############################# Make sure Alice can write her files")
    upload_file(
        alice_s3_client,
        bucket_name,
        "Alice",
        sentinel_file,
        custom_remote_path=f"{get_user_dir(bucket_name, 'Alice', with_bucket=False)}/SENTINEL_by_Alice",
    )


    print("############################# Make sure Bob can write his files")
    upload_file(
        bob_s3_client,
        bucket_name,
        "Bob",
        sentinel_file,
        custom_remote_path=f"{get_user_dir(bucket_name, 'Bob', with_bucket=False)}/SENTINEL_by_Bob",
    )


    print("############################# Make sure Alice can read her new files")
    list_files(
        alice_s3_client,
        bucket_name,
        get_user_dir(bucket_name, "Alice", with_bucket=False) + "/",
    )


    print("############################# Make sure Bob can read his new files")
    list_files(
        bob_s3_client,
        bucket_name,
        get_user_dir(bucket_name, "Bob", with_bucket=False) + "/",
    )


    print("############################# Make sure Bob cannot read Alice's files")
    list_files(
        bob_s3_client,
        bucket_name,
        get_user_dir(bucket_name, "Alice", with_bucket=False) + "/",
    )

    print("############################# Make sure Alice cannot read Bob's files")

    list_files(
        alice_s3_client,
        bucket_name,
        get_user_dir(bucket_name, "Bob", with_bucket=False) + "/",
    )



def get_iam_client(args, access_key, secret_key):
    iam_client = boto3.client(
        "iam",
        endpoint_url=args.iam_url,
        region_name=REGION_NAME,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return iam_client


def get_s3_client(args, access_key, secret_key):
    s3_client = boto3.client(
        "s3",
        endpoint_url=args.s3_url,
        region_name=REGION_NAME,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return s3_client


if __name__ == "__main__":
    main()
