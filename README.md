# ghg-emissions
Analytical and engineering project using GCP and dbt Cloud

## (Hypothetical) problem definition
TLDR: Comparing emissions reported on FAO portal and Climate Watch portal.

## Exploring available data
What can be extracted. Are there some areas that can be compared?
Testing the availability on datapoints for Canada. Add the Google Sheets report.

## Ingesting data

### FAO data
Manula download + cloud storage triggering google function that transforms the data and uploads them to the Google BigQuery dataset.

### Climate Watch data
Google Cloud Function with parameters that are used in API calls to Climate Watch portal (`start_year`, `end_year`, `regions`). The function transforms the data and uploads them to the Google BigQuery dataset.

## Creating models
Using dbt cloud.

### Data quality models
Checking reliability and completeness.
Summarizing findings in one master model.
Result - tests don't pass.

### Core models
Models that aim to compare the FAO data and Climate Watch data.

## Visualizing the findings
