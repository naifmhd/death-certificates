#!/usr/bin/env python

# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import logging
from google.cloud import storage, bigquery
from io import BytesIO
import sqlalchemy
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_bucket_blob(full_path):
    match = re.match(r'gs://([^/]+)/(.+)', full_path)
    bucket_name = match.group(1)
    blob_name = match.group(2)
    return bucket_name, blob_name


def sample_handler(storage_client, bucket, filein):
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.get_blob(filein)
    return blob.download_as_string(client=storage_client)


def create_table(bq_client, dataset, table_name, schema):
    dataset_ref = bq_client.dataset(dataset)
    table_ref = dataset_ref.table(table_name)

    try:
        table = bq_client.get_table(table_ref)
        raise ValueError('Table should not exist: {}'.format(table_name))
    except:
        pass

    table = bigquery.Table(table_ref, schema=schema)
    table = bq_client.create_table(table)
    return table


def copy_blob(bucket_name, blob_name, new_bucket_name, new_blob_name, service_account):
    """Copies a blob from one bucket to another with a new name."""
    storage_client = storage.Client.from_service_account_json(service_account)
    source_bucket = storage_client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(new_bucket_name)

    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, new_blob_name)

    print('Blob {} in bucket {} copied to blob {} in bucket {}.'.format(
        source_blob.name, source_bucket.name, new_blob.name,
        destination_bucket.name))


def save_to_bq(bq_dataset, bq_table, rows_to_insert, service_account, _create_table=True, schema=None):
    """Writes data to a BigQuery dataset.

    Args:
      bq_dataset: Name of the BigQuery dataset (string).
      bq_table: Name of the BigQuery table (string).
      rows_to_insert: One of: list of tuples/list of dictionaries). Row data to be inserted.
        If a list of tuples is given, each tuple should contain data for each schema field on the current table
        and in the same order as the schema fields. If a list of dictionaries is given, the keys must include all
        required fields in the schema. Keys which do not correspond to a field in the schema are ignored.
      service_account: Service account of BigQuery
      _create_table: Whether to create the table (default = True).
      schema: Schema of the data (list of `SchemaField`). Required if we create_table=True.
    """
    bq_client = bigquery.Client.from_service_account_json(service_account)

    if _create_table:
        if not schema:
            raise ValueError('Schema is required when creating the table')
        table = create_table(bq_client, bq_dataset, bq_table, schema)
        print(rows_to_insert)
        print('Table created')

    dataset_ref = bq_client.dataset(bq_dataset)
    table_ref = dataset_ref.table(bq_table)
    try:
        table = bq_client.get_table(table_ref)
    except:
        raise ValueError('Table {} does not exist.'.format(bq_table))
    load_job = bq_client.insert_rows(table, rows_to_insert)


def download_string(full_path):
    """Downloads the content of a gcs file."""
    storage_client = storage.Client()
    bucket_name, path = get_bucket_blob(full_path)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(path)
    byte_stream = BytesIO()
    blob.download_to_file(byte_stream)
    byte_stream.seek(0)
    return byte_stream


def save_to_db(row_to_insert, png_path, config):
    """Writes data to a BigQuery dataset.

    Args:
      bq_dataset: Name of the BigQuery dataset (string).
      bq_table: Name of the BigQuery table (string).
      rows_to_insert: One of: list of tuples/list of dictionaries). Row data to be inserted.
        If a list of tuples is given, each tuple should contain data for each schema field on the current table
        and in the same order as the schema fields. If a list of dictionaries is given, the keys must include all
        required fields in the schema. Keys which do not correspond to a field in the schema are ignored.
      service_account: Service account of BigQuery
      _create_table: Whether to create the table (default = True).
      schema: Schema of the data (list of `SchemaField`). Required if we create_table=True.
    """
    # bq_client = bigquery.Client.from_service_account_json(service_account)
    print(row_to_insert['file'])
    temp_path = png_path.split("/")[-1]
    db = sqlalchemy.create_engine(
        # Equivalent URL:
        # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=/cloudsql/<cloud_sql_instance_name>
        sqlalchemy.engine.url.URL(
            drivername="mysql+pymysql",
            username=config["mysql"]["user"],
            password=config["mysql"]["password"],
            database=config["mysql"]["database"],
            query={
                "unix_socket": "/cloudsql/{}".format(config["mysql"]["instance"])},
        ),
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800,
    )

    # if _create_table:
    #     if not schema:
    #         raise ValueError('Schema is required when creating the table')
    #     table = create_table(bq_client, bq_dataset, bq_table, schema)
    #     print(rows_to_insert)
    #     print('Table created')
    stmt = sqlalchemy.text(
        "INSERT INTO data (file, name, address, age, location, time, death_date, contact_number)" " VALUES (:file, :name, :address, :age, :location, :time, :death_date, :contact_number)"
    )
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            conn.execute(stmt, file=temp_path, name=row_to_insert['name'], address=row_to_insert['address'], age=row_to_insert['age'],
                         location=row_to_insert['location'], time=row_to_insert['time'], death_date=row_to_insert['death_date'], contact_number=row_to_insert['contact_number'])
    except Exception as e:
        print("Unable to successfully cast vote! Please check the ")

    print("Saved to DB successfully")
