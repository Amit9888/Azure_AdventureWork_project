CREATE MASTER KEY  ENCRYPTION BY PASSWORD ='Newgreenapple@12345' 

Create DATABASE SCOPED CREDENTIAL cred_ammy
with IDENTITY ='Managed Identity'


CREATE EXTERNAL DATA SOURCE source_silver
with (


    LOCATION ='https://awdatastoragelake.blob.core.windows.net/silver',
    CREDENTIAL = cred_ammy
)



CREATE EXTERNAL DATA SOURCE source_gold
with (


    LOCATION ='https://awdatastoragelake.blob.core.windows.net/gold',
    CREDENTIAL = cred_ammy
)



CREATE EXTERNAL FILE FORMAT format_parquet

WITH

(

    FORMAT_TYPE = PARQUET,

    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)



CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
        DATA_SOURCE = source_gold,
        FILE_FORMAT = format_parquet
)
AS

Select * from gold.sales

