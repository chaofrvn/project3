import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs() -> Path:
    """Download trip data from GCS"""
    gcs_path = f'data/rde.parquet'
    gcs_block = GcsBucket.load('zc-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path='./')
    return Path(gcs_path)

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df['passenger_count'].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load('zc-gcp-credentials')
    df.to_gbq(
        destination_table='huythai_prj3.reddit-prj3',
        project_id='eighth-parity-407109',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='replace'
    )
@flow()
def post_gcs_to_bq():
    """Main ETL flow to load data into BigQuery"""
    path = extract_from_gcs()
    df = transform(path)
    write_bq(df)

if __name__ == '__main__':
    post_gcs_to_bq()