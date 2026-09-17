## Region

- Region: ap-south-1

---

## SNS

- SNS Arn: arn:aws:sns:{REGION}:{ACCOUNT_ID}:weather-pipeline-sns-dev

---

## S3

- Bronze bucket: weather-pipeline-bronze-ap-south-1-dev
- Silver bucket: weather-pipeline-silver-ap-south-1-dev
- Gold bucket: weather-pipeline-gold-ap-south-1-dev
- Athena bucket: weather-pipeline-athena-ap-south-1-dev

---

## Glue

### Databases

- Bronze: weather_pipeline_bronze_dev
- Silver: weather_pipeline_silver_dev
- Gold: weather_pipeline_gold_dev

### Crawlers

- Bronze: weather-pipeline-bronze-crawler-dev

### ETL Jobs

- weather-pipeline-bronze-to-silver
  - --bronze_database: weather_pipeline_bronze_dev
  - --bronze_table: raw
  - --silver_bucket: weather-pipeline-silver-ap-south-1-dev
  - --silver_database: weather_pipeline_silver_dev
  - --silver_table: clean_observations

---
