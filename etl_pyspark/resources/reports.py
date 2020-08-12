report_queries = {
  "FLIGHT_DELAY_ETL": [
    {"REPAIR_FLIGHT_TRIPS":  "MSCK REPAIR TABLE airlines.flight_trips"},
    {"PROCESS_AND_LOAD_FLIGHT_COUNT": "INSERT OVERWRITE TABLE airlines.flight_count PARTITION (flightmonth) SELECT  concat(Year, '-', lpad(Month, 2, 0), '-', lpad(DayOfMonth, 2, 0)) AS FlightDate, count(1) AS FlightCount, sum(CASE WHEN IsDepDelayed = 'YES' THEN 1 ELSE 0 END) AS DepDelayedCount, sum(CASE WHEN IsArrDelayed = 'YES' THEN 1 ELSE 0 END) AS ArrDelayedCount, cast(concat(Year, lpad(Month, 2, 0)) AS INT) AS FlightMonth FROM airlines.flight_trips WHERE cast(concat(Year, lpad(Month, 2, 0)) AS INT) = {batch_month} GROUP BY cast(concat(Year, lpad(Month, 2, 0)) AS INT), FlightDate"}
  ],
  "REPORT_2": [
    {"QUERY_1": {"process.statement": "SELECT current_date()"}},
    {"QUERY_2": {"process.statement": "SELECT current_timestamp()"}}
  ]
}