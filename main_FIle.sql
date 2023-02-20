Use weather_forecast;

# Creating Table
CREATE TABLE weather_dataset (idx int not null Primary key auto_increment, Date date not null, Average_Temperature DOUBLE NOT NULL, Average_Humidity DOUBLE NOT NULL, 
Average_Dewpoint DOUBLE NOT NULL, Average_Barometer DOUBLE NOT NULL,Average_Windspeed DOUBLE NOT NULL, Average_Gustspeed DOUBLE NOT NULL, Average_Direction DOUBLE NOT NULL,
Rainfall_for_Month DOUBLE NOT NULL,Rainfall_for_Year DOUBLE NOT NULL, Maximum_rain_per_minute DOUBLE NOT NULL, Maximum_Temperature DOUBLE NOT NULL,
Minimum_Temperature DOUBLE NOT NULL, Maximum_Humidity DOUBLE NOT NULL,Minimum_Humidity DOUBLE NOT NULL, Maximum_Pressure DOUBLE NOT NULL,
Minimum_Pressure DOUBLE NOT NULL,Maximum_Wind_Speed DOUBLE NOT NULL,Maximum_Gust_Speed DOUBLE NOT NULL,Maximum_Heat_Index DOUBLE NOT NULL);

#Load Data from CSV file
LOAD DATA INFILE 'cleaned_weather_dataset.csv'
INTO TABLE weather_dataset  
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Date, Average_Temperature, Average_Humidity, Average_Dewpoint,
Average_Barometer, Average_Windspeed, Average_Gustspeed, Average_Direction,
Rainfall_for_Month, Rainfall_for_Year, Maximum_rain_per_minute, Maximum_Temperature,
Minimum_Temperature, Maximum_Humidity, Minimum_Humidity, Maximum_Pressure, 
Minimum_Pressure, Maximum_Wind_Speed, Maximum_Gust_Speed, Maximum_Heat_Index);

#Q.1. Give the count of the minimum number of days for the time when temperature reduced.
#     (assuming temperature reduced means when the minimum temperature is less than the overall minimum temperature in the table)

SELECT COUNT(*) as No_of_days
FROM (
  SELECT DATE, minimum_temperature
  FROM weather_dataset
  where minimum_temperature < (SELECT avg(minimum_temperature) FROM weather_dataset)
) SQ;

#Q.2. Find the temperature as Cold / hot by using the case and avg of values of the given data set. (Comparing daily temperature with its monthly average temperature)

SELECT 
  Date, 
  average_temperature AS daily_avg_temp,
  AVG(Average_Temperature) OVER (PARTITION BY EXTRACT(MONTH FROM Date), EXTRACT(YEAR FROM Date)) AS monthly_avg_temp,
  CASE 
    WHEN average_temperature >= AVG(Average_Temperature) OVER (PARTITION BY EXTRACT(MONTH FROM Date), EXTRACT(YEAR FROM Date)) 
    THEN 'HOT' 
    ELSE 'COLD' 
  END AS temp_status
FROM Weather_dataset
GROUP BY Date 
ORDER BY Date;

#Q.3. Can you check for all 4 consecutive days when the temperature was below 30 Fahrenheit? 
WITH table1 AS (
  SELECT idx,date, average_temperature temp,
         SUM(average_temperature < 30) OVER (ORDER BY idx ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cnt
  FROM weather_dataset
), table2 as(
SELECT idx,date, temp
FROM table1
WHERE cnt = 4)
select distinct table1.date Consecutive_Dates, table1.temp Temperature from table1,table2 where table1.idx in (table2.idx, table2.idx-1, table2.idx-2, table2.idx-3); 

#Q.4. Can you find the maximum number of days for which temperature dropped. 
#	  (Assuming temperature dropped means if the daily temperature is lower than the previous day's temperature.)

WITH daily_temperature_change AS (
  SELECT date, Average_Temperature as daily_temperature, LAG(Average_Temperature) OVER (ORDER BY date) AS previous_temperature
  FROM weather_dataset
)
SELECT 
  COUNT(*) AS num_days
FROM 
  daily_temperature_change
WHERE 
  daily_temperature < previous_temperature;
  
#Q.5. Can you find the average humidity average from the dataset
#     ( NOTE:should contain the following clauses: group by, order by, date ) 

SELECT CONCAT(EXTRACT(Month FROM Date),'-',EXTRACT(YEAR FROM Date)) as Month_Year, AVG(Average_Humidity) 
FROM weather_dataset  
GROUP BY EXTRACT(YEAR FROM Date), EXTRACT(Month FROM Date) 
ORDER BY date;

#Q.6. Use the GROUP BY clause on the Date column and make a query to fetch details for average windspeed ( which is now windspeed done in task 3 )

SELECT 
  CONCAT(EXTRACT(Month FROM Date),'-',EXTRACT(YEAR FROM Date)) as Month_Year,
  AVG(Average_windspeed) AS avg_windspeed
FROM weather_dataset
group by EXTRACT(YEAR FROM Date), EXTRACT(Month FROM Date) 
ORDER BY date;

#Q.7. Please add the data in the dataset for 2034 and 2035 as well as forecast predictions for these years
#     ( NOTE:data consistency and uniformity should be maintained )

LOAD DATA INFILE 'MOCK_DATA.csv'
INTO TABLE weather_dataset  
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Date, Average_Temperature, Average_Humidity, Average_Dewpoint,
Average_Barometer, Average_Windspeed, Average_Gustspeed, Average_Direction,
Rainfall_for_Month, Rainfall_for_Year, Maximum_rain_per_minute, Maximum_Temperature,
Minimum_Temperature, Maximum_Humidity, Minimum_Humidity, Maximum_Pressure, 
Minimum_Pressure, Maximum_Wind_Speed, Maximum_Gust_Speed, Maximum_Heat_Index);


#Q.8. If the maximum gust speed increases from 55mph, fetch the details for the next 4 days.

with table1 as (SELECT idx, date, Maximum_Gust_Speed
FROM weather_dataset
WHERE Maximum_Gust_Speed > 55)
select distinct t.* from weather_dataset t, table1 where t.idx in (table1.idx+1, table1.idx+2, table1.idx+3, table1.idx+4);

#Q.9. Find the number of days when the temperature went below 0 degrees Celsius.( Since 0 deg Celsius = 32 deg Fahrenheit)
SELECT COUNT(*) No_of_days
FROM weather_dataset
WHERE Average_temperature < 32;

#Q.10. Create another table with a “Foreign key” relation with the existing given data set.
CREATE TABLE daily_difference_pressure (
  new_table_id INT PRIMARY KEY AUTO_INCREMENT,
  pressure float,
  dateinfo date,
  FOREIGN KEY (new_table_id) REFERENCES weather_dataset(idx)
);
INSERT INTO daily_difference_pressure (dateinfo, pressure)
SELECT DATE, (maximum_pressure - minimum_pressure) AS pressure
FROM weather_dataset;