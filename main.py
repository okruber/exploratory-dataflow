import apache_beam as beam
import csv
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
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


def parse_arguments():
    """
    Parse command line arguments for the flex template.
    
    Returns:
        Tuple: (known_args, pipeline_options)
    """
    # Parse the custom arguments first
    parser = argparse.ArgumentParser()
    
    # Add the arguments as defined in metadata.json
    parser.add_argument(
        '--input_path',
        required=True,
        help='The path and filename for input CSV file. Example: gs://totemic-bucket-raw/covid_19_data.csv'
    )
    parser.add_argument(
        '--output_path',
        required=True,
        help='The path and filename prefix for writing the output. Example: gs://totemic-bucket-trusted/covid_19_data'
    )
    
    known_args, pipeline_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(pipeline_args)

    # for csv module issue
    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True
    
    return known_args, pipeline_options


def run(known_args, pipeline_options):
    """
    Run the Apache Beam pipeline to read CSV from one GCS bucket and write
    to another bucket in Parquet format.
    
    Args:
        known_args: Parsed arguments containing input_path and output_path
        pipeline_options: Pipeline options for Beam
    """
    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read the CSV file
        csv_data = (
            pipeline
            | "Read CSV from GCS" >> beam.io.ReadFromText(known_args.input_path, skip_header_lines=1)
            | "Parse CSV Lines" >> beam.Map(parse_csv_line)
        )
        
        # Write the data to Parquet format
        csv_data | "Write to Parquet" >> beam.io.parquetio.WriteToParquet(
            known_args.output_path,
            create_parquet_schema(),
            file_name_suffix=".parquet"
        )


if __name__ == "__main__":
    known_args, pipeline_options = parse_arguments()
    run(known_args, pipeline_options)