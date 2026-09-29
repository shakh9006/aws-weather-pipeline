SELECT observation_date, avg_temp, min_temp, max_temp, avg_humidity, observations
FROM city_daily_weather
WHERE city = 'london'
ORDER BY observation_date;