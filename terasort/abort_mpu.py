import boto3
import sys

BUCKET = "terasort"

if __name__ == '__main__':
    s3_client = boto3.client("s3", endpoint_url="http://localhost:9000")
    
    results = s3_client.list_multipart_uploads(Bucket=BUCKET)
    mpus = results.get('Uploads')
    
    if mpus is not None:
        for mpu in mpus:
            # print("Deleting: ", mpu['Key'])
            s3_client.abort_multipart_upload(Bucket=BUCKET, Key=mpu['Key'], UploadId=mpu['UploadId'])
        