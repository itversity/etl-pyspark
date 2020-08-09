CREATE EXTERNAL TABLE `airlines`.`flight_trips` (`Year` INT, `Month` INT, `DayofMonth` INT, `DayOfWeek` INT, `DepTime` STRING, `CRSDepTime` INT, `ArrTime` STRING, `CRSArrTime` INT, `UniqueCarrier` STRING, `FlightNum` INT, `TailNum` STRING, `ActualElapsedTime` STRING, `CRSElapsedTime` INT, `AirTime` STRING, `ArrDelay` STRING, `DepDelay` STRING, `Origin` STRING, `Dest` STRING, `Distance` STRING, `TaxiIn` STRING, `TaxiOut` STRING, `Cancelled` INT, `CancellationCode` STRING, `Diverted` INT, `CarrierDelay` STRING, `WeatherDelay` STRING, `NASDelay` STRING, `SecurityDelay` STRING, `LateAircraftDelay` STRING, `IsArrDelayed` STRING, `IsDepDelayed` STRING)
PARTITIONED BY (FlightMonth INT)
LOCATION '/Users/itversity/Research/data/airlines-part'
STORED AS parquet;

CREATE EXTERNAL TABLE airlines.flight_count(FlightDate DATE,
    FlightCount INT,
    DepDelayedCount INT,
    ArrDelayedCount INT
) PARTITIONED BY (flightmonth INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/Users/itversity/Research/data/airlines/flight_count';

CREATE EXTERNAL TABLE `airlines`.`flight_trips` (`Year` INT, `Month` INT, `DayofMonth` INT, `DayOfWeek` INT, `DepTime` STRING, `CRSDepTime` INT, `ArrTime` STRING, `CRSArrTime` INT, `UniqueCarrier` STRING, `FlightNum` INT, `TailNum` STRING, `ActualElapsedTime` STRING, `CRSElapsedTime` INT, `AirTime` STRING, `ArrDelay` STRING, `DepDelay` STRING, `Origin` STRING, `Dest` STRING, `Distance` STRING, `TaxiIn` STRING, `TaxiOut` STRING, `Cancelled` INT, `CancellationCode` STRING, `Diverted` INT, `CarrierDelay` STRING, `WeatherDelay` STRING, `NASDelay` STRING, `SecurityDelay` STRING, `LateAircraftDelay` STRING, `IsArrDelayed` STRING, `IsDepDelayed` STRING)
PARTITIONED BY (FlightMonth INT)
LOCATION 'dbfs:/FileStore/tables/airlines-part'
STORED AS parquet;

CREATE EXTERNAL TABLE airlines.flight_count(FlightDate DATE,
    FlightCount INT,
    DepDelayedCount INT,
    ArrDelayedCount INT
) PARTITIONED BY (flightmonth INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'dbfs:/FileStore/tables/flight_count';

CREATE EXTERNAL TABLE `airlines`.`flight_trips` (`Year` INT, `Month` INT, `DayofMonth` INT, `DayOfWeek` INT, `DepTime` STRING, `CRSDepTime` INT, `ArrTime` STRING, `CRSArrTime` INT, `UniqueCarrier` STRING, `FlightNum` INT, `TailNum` STRING, `ActualElapsedTime` STRING, `CRSElapsedTime` INT, `AirTime` STRING, `ArrDelay` STRING, `DepDelay` STRING, `Origin` STRING, `Dest` STRING, `Distance` STRING, `TaxiIn` STRING, `TaxiOut` STRING, `Cancelled` INT, `CancellationCode` STRING, `Diverted` INT, `CarrierDelay` STRING, `WeatherDelay` STRING, `NASDelay` STRING, `SecurityDelay` STRING, `LateAircraftDelay` STRING, `IsArrDelayed` STRING, `IsDepDelayed` STRING)
PARTITIONED BY (FlightMonth INT)
LOCATION '/user/training/airlines/airlines-part'
STORED AS parquet;

CREATE EXTERNAL TABLE airlines.flight_count(FlightDate DATE,
    FlightCount INT,
    DepDelayedCount INT,
    ArrDelayedCount INT
) PARTITIONED BY (flightmonth INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/training/airlines/flight_count';