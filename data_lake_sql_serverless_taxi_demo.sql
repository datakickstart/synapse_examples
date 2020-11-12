--------------------
-- Create Data Source
-------------------

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'demo_adls') 
	CREATE EXTERNAL DATA SOURCE [demo_adls] 
	WITH (
		LOCATION   = 'https://dvtrainingadls.dfs.core.windows.net/demo'
	)
Go

--------------------------------------
-- Simple query without external table (3s)
--------------------------------------
SELECT TOP 10 *
FROM OPENROWSET(BULK 'nyc_taxi/yellow_trips_detail/year_month=2018_01/*.snappy.parquet',
                DATA_SOURCE = 'demo_adls',
      			FORMAT='PARQUET') AS nyc
WHERE VendorID = 2


-------------------------
-- Parquet external table
-------------------------
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'yellow_trips_detail') 
	DROP EXTERNAL TABLE yellow_trips_detail

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'Parquet') 
	CREATE EXTERNAL FILE FORMAT Parquet WITH (  FORMAT_TYPE = PARQUET )

CREATE EXTERNAL TABLE yellow_trips_detail (
	[VendorID] int,
	[tpep_pickup_datetime] datetime2(7),
	[tpep_dropoff_datetime] datetime2(7),
	[passenger_count] int,
	[trip_distance] float,
	[RatecodeID] int,
	[store_and_fwd_flag] varchar(8000),
	[PULocationID] int,
	[DOLocationID] int,
	[payment_type] int,
	[fare_amount] float,
	[extra] float,
	[mta_tax] float,
	[tip_amount] float,
	[tolls_amount] float,
	[improvement_surcharge] float,
	[total_amount] float,
	[pickup_dt] datetime2(7),
	[dropoff_dt] datetime2(7)
	)
	WITH (
	LOCATION = 'nyc_taxi/yellow_trips_detail/year_month=2018_01/part-*.snappy.parquet',
	DATA_SOURCE = [demo_adls],
	FILE_FORMAT = [Parquet]
	)
GO

-- Sample top 10 (3s)
SELECT TOP 10 * FROM yellow_trips_detail


-- Aggregate and filter to a single day (20s)
SELECT VendorId, cast(Tpep_pickup_datetime as date) as PickupDate, Sum(Trip_distance) as TotalTripDistance
FROM yellow_trips_detail
where cast(Tpep_pickup_datetime as date) = '2018-01-01'
GROUP BY VendorId, cast(Tpep_pickup_datetime as date)


--------------------
-- Create External Table As (7s)
-- NOTE: Before running, clear out Location using Azure Portal
-------------------
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_trips_daily') 
	DROP EXTERNAL TABLE ext_trips_daily

CREATE EXTERNAL TABLE ext_trips_daily
WITH ( 
    LOCATION = 'nyc_taxi/ext_trips_daily',
	DATA_SOURCE = [demo_adls],
	FILE_FORMAT = [Parquet]
)
AS
SELECT VendorId, cast(Tpep_pickup_datetime as date) as PickupDate, Sum(Trip_distance) as TotalTripDistance
FROM yellow_trips_detail
GROUP BY VendorId, cast(Tpep_pickup_datetime as date)
GO

-- Filter external table to a single day (2s)
SELECT * from ext_trips_daily where PickupDate = '2018-01-01'


-----------------
-- Create View
-----------------
CREATE OR ALTER VIEW vw_trips_daily AS
SELECT VendorId, cast(Tpep_pickup_datetime as date) as PickupDate, Sum(Trip_distance) as TotalTripDistance
FROM OPENROWSET(BULK 'nyc_taxi/yellow_trips_detail/year_month=2018_01/*.snappy.parquet',
                DATA_SOURCE = 'demo_adls',
      			FORMAT='PARQUET') AS nyc
GROUP BY VendorId, cast(Tpep_pickup_datetime as date)
GO

-- Filter view to a single day (6s)
SELECT * from vw_trips_daily where PickupDate = '2018-01-01'


-- --------------------
-- -- CSV external table
-- -------------------

-- IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'Csv') 
-- 	CREATE EXTERNAL FILE FORMAT Csv WITH (  
-- 		FORMAT_TYPE = DELIMITEDTEXT,
--     	FORMAT_OPTIONS ( FIELD_TERMINATOR = ',', STRING_DELIMITER = '"', FIRST_ROW = 2   ))
-- GO

-- IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'trips_external_csv') 
-- 	DROP EXTERNAL TABLE trips_external_csv
-- Go


-- CREATE EXTERNAL TABLE trips_external_csv
-- (
-- VendorID int,
-- tpep_pickup_datetime varchar,
-- tpep_dropoff_datetime varchar,
-- passenger_count int,
-- trip_distance float,
-- RatecodeID int,
-- store_and_fwd_flag varchar,
-- PULocationID int,
-- DOLocationID int,
-- payment_type int,
-- fare_amount float,
-- extra float,
-- mta_tax float,
-- tip_amount int,
-- tolls_amount int,
-- improvement_surcharge float,
-- total_amount float,
-- congestion_surcharge varchar
-- )
-- WITH (
-- 	LOCATION = 'nyctaxi/tripdata/yellow/2019/yellow_tripdata_2019-01.csv.gz',
-- 	DATA_SOURCE = [demo_adls],
-- 	FILE_FORMAT = [Csv]
-- 	)
-- GO


-- SELECT TOP 10 * FROM trips_external_csv
-- GO
