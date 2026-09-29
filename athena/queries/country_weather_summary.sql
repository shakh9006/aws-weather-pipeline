SELECT country, city, avg_temp, peak_temp, lowest_temp, warmth_rank
FROM country_weather_summary
ORDER BY avg_temp DESC
LIMIT 20;