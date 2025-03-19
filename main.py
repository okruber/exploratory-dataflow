import apache_beam as beam
import csv
import argparse
import time
import datetime
import json
import logging
import os
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.gcsio import GcsIO
import pyarrow as pa


def parse_csv_line(line):
    """
    Parse a CSV line into a dictionary using the provided schema.
    
    Args:
        line (str): A single line from the CSV file
        
    Returns:
        dict: A dictionary with keys from the schema and values from the CSV line
    """
    # Split the CSV line while handling quoted values
    reader = csv.reader([line])
    values = next(reader)
    
    # Map values to schema keys
    return {
        "SNo": int(values[0]) if values[0] else None,
        "ObservationDate": values[1],
        "Province/State": values[2],
        "Country/Region": values[3],
        "LastUpdate": values[4],
        "Confirmed": float(values[5]) if values[5] else None,
        "Deaths": float(values[6]) if values[6] else None,
        "Recovered": float(values[7]) if values[7] else None
    }


def create_file_path(destination_bucket, source_system, dataset, pattern):
    """
    Create a file path prefix according to the specified format.
    
    Args:
        destination_bucket (str): The destination GCS bucket in format gs://bucket-name
        source_system (str): The source system name
        dataset (str): The dataset name
        
    Returns:
        str: A file path prefix formatted as:
            gs://bucket-name/source_system/dataset/year=YYYY/month=MM/day=DD/source_system-dataset-YYYYMMDDHHMMSS.batch.EPOCHTIME
    """
    # Strip trailing slash from bucket if present
    if destination_bucket.endswith('/'):
        destination_bucket = destination_bucket[:-1]
    
    # Get the current time
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y%m%d%H%M%S")
    epoch_time = int(time.time())
    
    # Create the directory path with year, month, day
    dir_path = f"{destination_bucket}/{source_system}/{dataset}/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
    
    # Create the file name prefix
    # TODO: parameterize file format and compression, add domain
    file_name_prefix = f"{source_system}-{dataset}-{timestamp}.{pattern}.{epoch_time}"
    
    # Combine the directory path and file name prefix
    return f"{dir_path}{file_name_prefix}"


def create_parquet_schema():
    """
    Create a PyArrow schema for writing Parquet files.
    
    Returns:
        pyarrow.Schema: A schema for the Parquet file
    """
    return pa.schema([
        ('SNo', pa.int64()),
        ('ObservationDate', pa.string()),
        ('Province/State', pa.string()),
        ('Country/Region', pa.string()),
        ('LastUpdate', pa.string()),
        ('Confirmed', pa.float64()),
        ('Deaths', pa.float64()),
        ('Recovered', pa.float64())
    ])


def read_pipeline_config(config_path):
    """
    Read and parse the pipeline configuration JSON file from either GCS or local file system.
    
    Args:
        config_path (str): The path to the pipeline_config.json file
        
    Returns:
        dict: The parsed configuration as a dictionary
    """
    # todo: workflow to upload pipeline_config.json to GCS flex template bucket
    try:
        if config_path.startswith('gs://'):
            gcs_client = GcsIO()
            with gcs_client.open(config_path) as f:
                return json.loads(f.read().decode('utf-8'))
        else:
            with open(config_path, 'r') as config_file:
                return json.load(config_file)
    except Exception as e:
        logging.error(f"Error reading configuration file {config_path}: {e}")
        raise


def parse_arguments():
    """
    Parse command line arguments for the flex template.
    
    Returns:
        Tuple: (config, pipeline_options)
    """
    # Parse the custom arguments first
    parser = argparse.ArgumentParser()
    
    # supplied via GUI
    parser.add_argument(
        '--pipeline_config',
        required=True,
        help='The path to the JSON configuration file containing pipeline parameters. Can be a GCS path (gs://) or local file path.'
    )
    
    known_args, pipeline_args = parser.parse_known_args()
    
    config = read_pipeline_config(known_args.pipeline_config)
    
    # config validation
    if 'input_path' not in config:
        raise ValueError("input_path is required but not provided in pipeline_config")
    if 'destination_bucket' not in config:
        raise ValueError("destination_bucket is required but not provided in pipeline_config")
    
    pipeline_options = PipelineOptions(pipeline_args)

    # for csv module issue - remove later
    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True
    
    return config, pipeline_options


def run(config, pipeline_options):
    """
    Run the Apache Beam pipeline to read CSV from one GCS bucket and write
    to another bucket in Parquet format.
    
    Args:
        config: Dictionary containing configuration parameters
        pipeline_options: Pipeline options for Beam
    """
    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        csv_data = (
            pipeline
            | "Read CSV from GCS" >> beam.io.ReadFromText(config['input_path'], skip_header_lines=1)
            | "Parse CSV Lines" >> beam.Map(parse_csv_line)
        )
        
        output_path_prefix = create_file_path(
            config['destination_bucket'],
            config['source_system'], 
            config['dataset'],
            config['pattern']
        )
        
        csv_data | "Write to Parquet" >> beam.io.parquetio.WriteToParquet(
            file_path_prefix=output_path_prefix,
            schema=create_parquet_schema(),
            file_name_suffix=".parquet",
            codec="gzip" # parameterize compression into suffix
        )


if __name__ == "__main__":
    config, pipeline_options = parse_arguments()
    run(config, pipeline_options)