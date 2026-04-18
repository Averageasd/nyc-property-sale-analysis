import boto3
import psycopg2
import os

client = boto3.client('redshift-data')

REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_DATABASE = os.getenv('REDSHIFT_DATABASE')
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
IAM_ROLE = os.getenv('IAM_ROLE')

def lambda_handler(event, context):
    record = event['Records'][0]
    bucket_name = record['s3']['bucket']['name']
    input_key = record['s3']['object']['key']
    s3_input_path = f's3://{bucket_name}/{input_key}'
    conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            dbname=REDSHIFT_DATABASE,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
    )

    cursor = conn.cursor()

    id_with_lower_hyphen = context.aws_request_id.replace('-', '_')

    merge_sql = f"""
        BEGIN;
        CREATE TABLE IF NOT EXISTS public.temp_nyc_sale_{id_with_lower_hyphen} (LIKE public.nyc_properties_sale);
        COPY public.temp_nyc_sale_{id_with_lower_hyphen}
        FROM '{s3_input_path}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        REGION 'us-east-1';

        MERGE INTO public.nyc_properties_sale USING public.temp_nyc_sale_{id_with_lower_hyphen} ON public.nyc_properties_sale.ID = public.temp_nyc_sale_{id_with_lower_hyphen}.ID REMOVE DUPLICATES;

        DROP TABLE IF EXISTS public.temp_nyc_sale_{id_with_lower_hyphen};

        COMMIT;
    """
    cursor.execute(merge_sql)
    conn.commit()