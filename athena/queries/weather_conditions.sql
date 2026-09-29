SELECT country, weather_main, occurrences, share_pct, avg_temp_in_condition
FROM weather_conditions
ORDER BY country, share_pct DESC;