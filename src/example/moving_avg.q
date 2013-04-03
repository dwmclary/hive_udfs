#All of these examples use the 2006 airline data used in previous R examples.

ADD JAR /mnt/shared/hive_udfs/dist/lib/moving_average_udf.jar;
CREATE TEMPORARY FUNCTION moving_avg AS 'com.oracle.hadoop.hive.ql.udf.generic.GenericUDAFMovingAverage';

CREATE EXTERNAL TABLE airlines
(Year int ,Month int,DayofMonth int,DayOfWeek int,DepTime int,
CRSDepTime int,ArrTime int,CRSArrTime int ,UniqueCarrier string ,FlightNum string ,TailNum string,
ActualElapsedTime int,CRSElapsedTime int ,AirTime int ,ArrDelay int ,DepDelay int,
Origin string,Dest string,Distance int,TaxiIn int ,TaxiOut int,Cancelled int,
CancellationCode int ,Diverted int ,CarrierDelay int,WeatherDelay int ,
NASDelay int ,SecurityDelay int,LateAircraftDelay int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/oracle/airlines';

#get sequential integers for time -- note, time series methods produce weird/bad/wrong results when you have more than one observation per time
CREATE TABLE ts_example AS 
SELECT CAST(UNIX_TIMESTAMP(CONCAT(CAST(Year as string),"-", 
  CAST(Month as string), "-",CAST(DayofMonth as string)), "yyyy-MM-dd") as int) as timestring,
  ArrDelay as delay, TailNum FROM airlines;


#get the moving average for a single tail number
SELECT TailNum,moving_avg(timestring, delay, 4) FROM ts_example WHERE TailNum='N967CA' GROUP BY TailNum LIMIT 100;

#create a set of moving averages for every plane starting with N
#Note: this UDAF blows up unpleasantly in heap -- there will be data volumes for which you need to throw
#excessive amounts of memory at the problem
CREATE TABLE moving_averages AS 
SELECT TailNum, moving_avg(timestring, delay, 4) as timeseries FROM ts_example 
WHERE TailNum LIKE 'N%' GROUP BY TailNum;

#extract the moving average and residuals
CREATE TABLE ts_comparison AS 
SELECT TailNum, CAST(ma.period as INT) as period, ma.moving_average as moving_average, ma.residual as residual FROM moving_averages
LATERAL VIEW explode(timeseries) result AS ma ORDER BY period ASC;
