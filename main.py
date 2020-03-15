import jpg2png
import yaml

config = yaml.safe_load(open("config.yaml", "r"))


def hello_gcs_generic(data, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.

    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    output_bucket = 'processed_kashunamaadhu'
    jpg2png.convert_jpgs(data=data, context=context,
                         output_bucket=output_bucket, config=config)
