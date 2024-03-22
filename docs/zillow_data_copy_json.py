import json
import boto3

# Important to call boto3 to call S3 client
s3_client = boto3.client("s3")


def lambda_handler(event, context):
    # TODO implement
    # This will only work once the new item is added. Once a new item is triggered.

    source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]

    # Defining the name of the bucket
    # Defining where the source needs to be copied
    target_bucket = "zillow-copy-of-raw-json-bucket"

    copy_soure = {"Bucket": source_bucket, "Key": object_key}

    # Using the command to check if the object exist or not
    # Then we will wait to check for the object and
    # then we will copy the object to the target_bucket

    waiter = s3_client.get_waiter("object_exists")
    waiter.wait(Bucket=source_bucket, Key=object_key)
    s3_client.copy_object(Bucket=target_bucket, Key=object_key, CopySource=copy_soure)

    # Returning the message with code 200 to check if it ran successfully or not
    return {"statusCode": 200, "body": json.dumps("File copied successfully!")}
