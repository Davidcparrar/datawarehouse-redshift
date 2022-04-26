import boto3
import configparser

# ---------------------------------------------------------------------
# --------------------- LOAD ENVIROMENT VARIABLES ---------------------
# ---------------------------------------------------------------------

config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

AWS_KEY = config.get("AWS", "AWS_KEY")
AWS_SECRET = config.get("AWS", "AWS_SECRET")
AWS_REGION = config.get("AWS", "AWS_REGION")


def main():
    """Check S3 raw files"""
    # Get resource
    s3 = boto3.resource(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )
    udacity = s3.Bucket("udacity-dend")

    # Check song files
    for it, obj in enumerate(udacity.objects.filter(Prefix="song_data")):
        print(obj.key)
        if it == 5:
            break

    udacity.download_file(obj.key, f"sample/{obj.key.split('/')[-1]}")

    # Check log files
    for it, obj in enumerate(udacity.objects.filter(Prefix="log_data")):
        print(obj.key)

    udacity.download_file(obj.key, f"sample/{obj.key.split('/')[-1]}")
    udacity.download_file("log_json_path.json", "sample/log_json_path.json")


if __name__ == "__main__":
    main()
