import json
import boto3
import pandas as pd


# Important to call boto3 to call S3 client
s3_client = boto3.client("s3")


def lambda_handler(event, context):
    # TODO implement

    # Get the S3 bucket name and object key
    source_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]

    # Defining the name of the bucket where we want the transformed files to be stored
    target_bucket_name = "zillow-transform-from-json-to-csv"

    # Grab the filename without the extension
    target_file_name = object_key[:-5]
    # print(target_file_name)

    # Checking that object has been created or not. It will wait till it gets created.
    waiter = s3_client.get_waiter("object_exists")
    waiter.wait(Bucket=source_bucket, Key=object_key)

    # Use getObject method from the S3 service to fetch the content of the JSON file
    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    # print(response)

    # Read the object's body
    content = response["Body"].read().decode("utf-8")

    # Loading the data in json format
    json_content = json.loads(content)
    # print(json_content)

    # Storing the data in string
    zillow_data = []
    for i in json_content["results"]:
        zillow_data.append(i)

    # Converting the data into dataframe
    zillow_df = pd.DataFrame(zillow_data)

    # Selecting only useful columns

    selected_columns = [
        "bathrooms",
        "bedrooms",
        "city",
        "daysOnZillow",
        "homeStatus",
        "homeType",
        "livingArea",
        "price",
        "rentZestimate",
        "streetAddress",
        "zipcode",
    ]

    zillow_df = zillow_df[selected_columns]
    print(zillow_df)

    # Convert dataframe into CSV
    csv_data = zillow_df.to_csv(index=False)

    # Upload CSV to S3
    # Body is known as the data
    object_key = f"{target_file_name}.csv"
    s3_client.put_object(Bucket=target_bucket_name, Key=object_key, Body=csv_data)

    # Displaying the message that it ran successfully
    return {"statusCode": 200, "body": json.dumps("Transformed into CSV successfully")}
