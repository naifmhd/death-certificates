# Copyright 2019 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Converts pdf files into png, outputs converted png files to the folder."""

import argparse
import json
import logging
import os
import re
import sys
import tempfile

import tensorflow as tf
from wand.image import Image

from google.cloud import storage, bigquery, vision

# TODO: Use tf.file_io --> removes the need to get bucket name.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def png2txt(png_path, txt_path, storage_client):
    """convert the png file to txt using OCR."""

    vision_client = vision.ImageAnnotatorClient()

    image = vision.types.Image()

    print("OCR processing {}".format(png_path))
    image.source.image_uri = png_path
    response = vision_client.text_detection(image=image)

    text = response.text_annotations[0].description
    tmp_filename = txt_path.split("/")[-1]

    temp_txt = "tmp/{}".format(tmp_filename)
    with open(temp_txt, "w") as f:
        f.write(text)
        f.close()

    print("Writing txt at: {}".format(txt_path))
    match = re.match(r"gs://([^/]+)/(.+)", txt_path)
    new_bucket_name = match.group(1)
    new_file_name = match.group(2)
    new_blob = storage_client.get_bucket(new_bucket_name).blob(new_file_name)
    new_blob.upload_from_filename(temp_txt)


def jpg2png2txt(current_blob,
                png_path,
                txt_path,
                storage_client):
    """convert the given file using ImageMagick."""

    # Create temp directory & all intermediate directories
    temp_directory = "/tmp/google"
    if not os.path.exists(temp_directory):
        os.makedirs(temp_directory)

    file_name = current_blob.name
    handler, temp_local_filename = tempfile.mkstemp(dir="tmp/")

    current_blob.download_to_filename(temp_local_filename)
    try:
        with Image(filename=temp_local_filename, resolution=300) as img:
            with img.convert("png") as converted:
                converted.save(filename=temp_local_filename.replace(".jpg",
                                                                    ".png"))
    except:
        print("Image seems corrupted: {}".format(file_name))
        return

    match = re.match(r"gs://([^/]+)/(.+)", png_path)
    new_bucket_name = match.group(1)
    new_file_name = match.group(2)
    new_blob = storage_client.get_bucket(new_bucket_name).blob(new_file_name)
    new_blob.upload_from_filename(temp_local_filename)

    # Convert png to txt
    png2txt(png_path=png_path,
            txt_path=txt_path,
            storage_client=storage_client)

    # Delete the temporary file.
    os.remove(temp_local_filename)


# def convert_jpgs(main_project_id,
#                  demo_dataset,
#                  input_path,
#                  service_acct):
#     """Converts jpgs to png.

#     Args:
#       input_path: Folder containing the jpgs (e.g. gs://...)
#       service_account: API key needed to access Cloud storage
#     """

#     # Set up client for BigQuery and GCS.
#     storage_client = storage.Client.from_service_account_json(
#         service_acct)
#     bq_client = bigquery.Client.from_service_account_json(
#         service_acct)

#     # Create the BQ dataset to collect prediction results
#     bq_client = bigquery.Client.from_service_account_json(
#         service_acct)
#     dataset_id = "{}.{}".format(bq_client.project, demo_dataset)
#     dataset = bigquery.Dataset(dataset_id)
#     dataset.location = "US"

#     # Check to see if the BQ dataset already exists before creating
#     all_datasets = list(bq_client.list_datasets())
#     if all_datasets:
#         all_dataset_ids = [dataset.dataset_id for dataset in all_datasets]
#         if demo_dataset in all_dataset_ids:
#             logger.error(
#                 f"The dataset named {demo_dataset} already exists in project {main_project_id}.")
#             logger.info(
#                 f"Enter a different dataset id in the config file or delete the existing '{demo_dataset}' dataset.")
#             sys.exit()

#     dataset = bq_client.create_dataset(dataset)  # API request
#     logger.info("Created dataset {}.{} to collect results.".format(
#         bq_client.project, dataset.dataset_id))

#     # Process sample patent jpgs
#     input_bucket_name = input_path.replace("gs://", "").split("/")[0]
#     bucket = storage_client.get_bucket(input_bucket_name)

#     folder_to_enumerate = input_path.replace(
#         input_bucket_name + "/", "").replace("gs://", "")
#     png_output_folder = f"gs://{input_bucket_name}/{demo_dataset}/png"
#     txt_output_folder = f"gs://{input_bucket_name}/{demo_dataset}/txt"

#     log_file_path = os.path.join(png_output_folder, "corrupted_files.txt")
#     log_file = tf.io.gfile.GFile(log_file_path, "w")

#     # storage_client = storage.Client.from_service_account_json(service_acct)
#     # blobs = storage_client.list_blobs(folder_to_enumerate, prefix='/')

#     logger.info(folder_to_enumerate)
#     for blob in storage_client.list_blobs(folder_to_enumerate):
#         logger.info(blob.name)
#         if (blob.name.endswith(".jpg")):
#             logger.info("Converting jpg: {}".format(blob.name))
#             current_blob = storage_client.get_bucket(
#                 input_bucket_name).get_blob(blob.name)
#             png_path = os.path.join(
#                 png_output_folder,
#                 os.path.basename(blob.name).replace(".jpg", ".png"))
#             txt_path = os.path.join(
#                 txt_output_folder,
#                 os.path.basename(blob.name).replace(".jpg", ".txt"))
#             logger.info("Writing png at: {}".format(png_path))
#             jpg2png2txt(current_blob=current_blob,
#                         png_path=png_path,
#                         txt_path=txt_path,
#                         storage_client=storage_client,
#                         service_acct=service_acct,
#                         log_file=log_file)
#     log_file.close()


def convert_jpgs(data, context):
    print('Converts jpgs to png')
    file_data = data

    file_name = file_data['name']
    bucket_name = file_data['bucket']

    storage_client = storage.Client()

    blob = storage_client.bucket(bucket_name).get_blob(file_name)
    blob_uri = f'gs://{bucket_name}/{file_name}'
    blob_source = {'source': {'image_uri': blob_uri}}
    print(blob_uri)
    """Converts jpgs to png.

    Args:
      input_path: Folder containing the jpgs (e.g. gs://...)
      service_account: API key needed to access Cloud storage
    """
    png_output_folder = f"gs://{bucket_name}/png"
    txt_output_folder = f"gs://{bucket_name}/txt"
    # storage_client = storage.Client.from_service_account_json(service_acct)
    # blobs = storage_client.list_blobs(folder_to_enumerate, prefix='/')

    png_path = os.path.join(
        png_output_folder,
        os.path.basename(file_name).replace(".jpg", ".png"))
    txt_path = os.path.join(
        txt_output_folder,
        os.path.basename(file_name).replace(".jpg", ".txt"))
    print("Writing png at: {}".format(png_path))
    jpg2png2txt(current_blob=blob,
                png_path=png_path,
                txt_path=txt_path,
                storage_client=storage_client)
