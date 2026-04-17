import json
import io
import hashlib
import pandas as pd
import pyarrow as pa
import boto3
import awswrangler as wr

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    record = event['Records'][0]
    bucket_name = record['s3']['bucket']['name']
    input_key = record['s3']['object']['key']
    file_extension = input_key.split('/')[1].split('.')[1]
    output_key = 'processed/' + input_key.split('/')[1].replace('.'+file_extension, '.parquet')
    file = "s3://"+bucket_name+"/"+input_key
    df = wr.s3.read_excel(file)
    df = remove_unecessary_header_text(df, file)
    clean_data(df)
    df['ID'] = df.apply(calculate_surrogate_key, axis=1).astype('string')
    save_parquet_to_s3(df, bucket_name, output_key)
    print(df)

def save_parquet_to_s3(df, bucket, key):
    wr.s3.to_parquet(
    df=df,
    path=f"s3://{bucket}/{key}",
    index=False,
    dataset=False,
    compression='snappy',
    dtype={
        'BOROUGH': 'string',
        'NEIGHBORHOOD': 'string',
        'BUILDING CLASS CATEGORY': 'string',
        'TAX CLASS BEFORE SALE': 'string',
        'BUILDING CLASS BEFORE SALE': 'string',
        'ADDRESS': 'string',
        'APARTMENT NUMBER': 'string',  
        'TAX CLASS AT TIME OF SALE': 'string',
        'BUILDING CLASS AT TIME OF SALE': 'string'
    }
)

def remove_unecessary_header_text(df, file):
    is_start_of_table = df.apply(lambda row: row.astype(str).str.contains('BOROUGH').any(), axis=1)
    header_idx = df.index[is_start_of_table][0]
    df = wr.s3.read_excel(file, skiprows = header_idx + 1)
    df.columns = df.columns.str.replace('\n', ' ').str.strip()
    df.columns = df.columns.str.replace('\s+', ' ', regex=True)
    cols = list(df.columns)
    cols[3] = "TAX CLASS BEFORE SALE" 
    cols[7] = "BUILDING CLASS BEFORE SALE"
    df.columns = cols
    return df

def clean_data(df):
    df['COMMERCIAL UNITS'] = df['COMMERCIAL UNITS'].fillna(0)
    df['RESIDENTIAL UNITS'] = df['RESIDENTIAL UNITS'].fillna(0)
    df['LAND SQUARE FEET'] = df['LAND SQUARE FEET'].fillna(0)
    df['GROSS SQUARE FEET'] = df['GROSS SQUARE FEET'].fillna(0)
    df['TOTAL UNITS'] = df['TOTAL UNITS'].fillna(0)
    df['TAX CLASS BEFORE SALE'] = df['TAX CLASS BEFORE SALE'].fillna('unknown')
    df['BUILDING CLASS BEFORE SALE'] = df['BUILDING CLASS BEFORE SALE'].fillna('unknown')
    df['TAX CLASS AT TIME OF SALE'] = df['TAX CLASS AT TIME OF SALE'].fillna('unknown')
    df['BUILDING CLASS AT TIME OF SALE'] = df['BUILDING CLASS AT TIME OF SALE'].fillna('unknown')
    df['APARTMENT NUMBER'] = df['APARTMENT NUMBER'].fillna('unknown')
    df = df.loc[~(df['YEAR BUILT'].isna()) & ~(df['YEAR BUILT'] == 0)]
    df = df.loc[~(df['ZIP CODE'] == 0) & ~(df['ZIP CODE'].isna())]

    df['COMMERCIAL UNITS'] = df['COMMERCIAL UNITS'].astype('int32')
    df['RESIDENTIAL UNITS'] = df['RESIDENTIAL UNITS'].astype('int32')
    df['LAND SQUARE FEET'] = df['LAND SQUARE FEET'].astype('float')
    df['GROSS SQUARE FEET'] = df['GROSS SQUARE FEET'].astype('float')
    df['TOTAL UNITS'] = df['TOTAL UNITS'].astype('int32')
    df['TAX CLASS BEFORE SALE'] = df['TAX CLASS BEFORE SALE'].astype('string')
    df['BUILDING CLASS BEFORE SALE'] = df['BUILDING CLASS BEFORE SALE'].astype('string')
    df['ADDRESS'] = df['ADDRESS'].astype('string')
    df['APARTMENT NUMBER'] = df['APARTMENT NUMBER'].astype('string')
    df['TAX CLASS AT TIME OF SALE'] = df['TAX CLASS AT TIME OF SALE'].astype('string')
    df['BUILDING CLASS AT TIME OF SALE'] = df['BUILDING CLASS AT TIME OF SALE'].astype('string')
    df = df.drop(columns=['EASE-MENT'])
    df = df.drop_duplicates()

def calculate_surrogate_key(row):
    unique_key = hashlib.md5((
        str(row['BOROUGH']) + 
        str(row['NEIGHBORHOOD']) +
        str(row['BUILDING CLASS CATEGORY']) +
        str(row['TAX CLASS BEFORE SALE']) +
        str(row['BLOCK']) + 
        str(row['LOT']) + 
        str(row['BUILDING CLASS BEFORE SALE']) +
        str(row['ADDRESS']) +
        str(row['APARTMENT NUMBER']) +
        str(row['ZIP CODE']) +
        str(row['RESIDENTIAL UNITS']) +
        str(row['COMMERCIAL UNITS']) +
        str(row['TOTAL UNITS']) +
        str(row['LAND SQUARE FEET']) +
        str(row['GROSS SQUARE FEET']) +
        str(row['YEAR BUILT']) +
        str(row['TAX CLASS AT TIME OF SALE']) + 
        str(row['BUILDING CLASS AT TIME OF SALE']) +
        str(row['SALE PRICE']) +
        str(row['SALE DATE'])
    ).encode('utf-8')).hexdigest()
    return unique_key