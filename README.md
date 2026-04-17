# NYC properties sales DE pipeline

## About
This project involves the design and implementation of an end-to-end AWS Serverless data engineering pipeline to ingest, transform, and analyze 21 years of New York City property sales data. By processing over two decades of records, the system provides a longitudinal view of the NYC market, enabling analysis of price-per-square-foot trends, borough-specific growth, and the long-term impact of economic cycles (2008 Financial Crisis, 2020 Pandemic) on the city's five boroughs.

## Tech stack
AWS Lambda, AWS Wranger, Pandas: Data Manipulation and Data Cleaning
</br>
AWS S3: Storage for raw .xls and .xlsx files and .parquet files used by Athena for ad-hoc analysis and Redshift for creating tables.
</br>
AWS Athena: Ad-hoc analysis. Make sure data is good before writing it into Redshift.
</br>
AWS Glue Crawler: Infer schema from processed folder to build database for Athena to use.
</br>
AWS Redshift: Warehouse that contains cleansed, sales data. 
</br>
Tableau: Data Visualization. Uses Data from Redshift. 