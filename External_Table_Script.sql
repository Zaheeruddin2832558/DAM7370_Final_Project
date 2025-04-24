CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'DamgFinalProject@7370';

CREATE DATABASE SCOPED CREDENTIAL cred_project
WITH IDENTITY = 'Managed Identity';

CREATE EXTERNAL DATA SOURCE source_silver
WITH (
    LOCATION = 'https://creditriskdataset.dfs.core.windows.net/silver',
    CREDENTIAL = cred_project
);

CREATE EXTERNAL DATA SOURCE source_gold
WITH (
    LOCATION = 'https://creditriskdataset.dfs.core.windows.net/gold',
    CREDENTIAL = cred_project
);

CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE = PARQUET
);


CREATE EXTERNAL TABLE gold.credit_risk_table
WITH (
    LOCATION = 'credit_risk_table/',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.credit_risk_raw;

