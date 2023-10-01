# Greenhouse gas emissions data comparison
This is an end-to-end data engineering and analytical project which aims to provide comparison of two different sources with greenhouse gas emissions data and visualize the greenhouse gas contributions of different sectors and agrifood systems in particular.

*DISCLAIMER: In order to focus on the data engineering and analytical tasks, I disregard some technical details about the datasets and make some intentional assumptions about them that might not be correct. Please use the results with caution.*

## Overview
The whole project was built using Google Cloud Platform services and dbt.

| ![Schema of services' integration](/assets/services_schema.png "Schema of services' integration") |
| --- |
| *Schema of services' integration* |

***Data sources***
* *Climate Watch* - data accessed using API call with optional parameters (from the [https://www.climatewatchdata.org/api/v1/data/historical_emissions](https://www.climatewatchdata.org/api/v1/data/historical_emissions) url address)
* *FAOSTAT* - data had to be downloaded manually (from the [https://www.fao.org/faostat/en/#data/GT](https://www.fao.org/faostat/en/#data/GT) url address)

***Data ingestion***
* Data were ingested using Google Cloud Function ETL scripts written in Python.
* Transformed data was stored in Google BigQuery data warehouse in a dataset called `ghg`.
* The scripts allow for incremental data refresh - if the source data (either requeted via API call or downloaded from the FAOSTAT website) was already in target tables, it got updated. No data is deleted in this process.
* The transformation scripts were slightly different for each data source.
  * *Climate Watch* [[cw-data-load](/scripts/cw-data-load)]
    * Data fetched in the script and pushed to `cw_data_stg` table.
    * Afterwards data was merged with target table `cw_data`.
  * *FAOSTAT* [[fao-data-load](/scripts/fao-data-load)]
    * The data was uploaded to a Google Cloud Storage bucket that triggered the ETL script.
    * The ETL script transformed and enriched the data.
    * Finally, the data was uploaded to `fao_data_stg` table and merged with `fao_data` table.
* Additional dimension tables were added to the `ghg` dataset

| ![Tables of the ghg dataset](/assets/bigquery_ghg.png "Tables of the ghg dataset") |
| --- |
| *Tables of the ghg dataset* |

***Data modeling***
* Data models were created using dbt (developed in dbt cloud).
* Intermediate models were stored in [[stg](/dbt/models/stg)] subfolder and deployed in `analytics_stg` dataset of the BigQuery project.
* There are models focusing on evaluating data quality - stored in [[data_quality](/dbt/models/data_quality)] subfolder and deployed in `analytics_data_quality` dataset.
* The main analytical models are in [[core](/dbt/models/core)] subfolder and deployed in `analytics` dataset.

| ![Tables and views of the analytical models](/assets/bigquery_analytics.png "Tables and views of the analytical models") |
| --- |
| *Tables and views of the analytical models* |

***Analysis and visualizations***
* The ooutput of the data modeling step were analyzed and visualized in Google Looker Studio.
* The full datasets included greenhouse gas emissions data from 1990 until 2020 (the development dataset was just a fraction of that - further described in the following text). 
* The full Google Looker Studio report can be found [here](https://lookerstudio.google.com/reporting/d80d2d2d-9fb4-44df-8e47-020009182925)

| ![Datasets comparison overview](/assets/looker_analysis.png "Datasets comparison overview") |
| --- |
| *Datasets comparison overview* |

## Detailed description of the development process

### 1 Context
Even though this is just a test project let's build some real foundations for its justification.

The scientists have already discovered in several studies that if the trend of global warming continues, there will be pretty bad consequences for human kind. Since we haven't invented any tools or mechanisms to cool down the planet, the only possible action we can take is to lower the amount of emitted greenhouse gases that contribute towards the warming of the planet.

One of the areas that might be ideal for optimisation is the food consumption. Usually it's almost no problem to get the products we want at a certain moment even though they were produced on the other side of the planet. The food availability causes some unnecessary greenhouse gases emission that are either related to the transportation of the goods or by over-production.

### 2 The project goal
Now the fun part begins. As mentioned in the disclaimer, there are some assumptions about the datasets and due to simplification some information in this section is made up.

Before making any changes like prohibiting international trade, we first want to see the composition of emissions related to different sectors in order to understand the significance of the food production. There is also suspicion that countries do not report their emissions correctly in order to look better compared to other countries. But luckily there is one organisation that have more precise numbers of emitted greenhouse gases so our goal is also to see how off the countries' reports are.

The goals:
1. Visualize the proportion of CO<sub>2</sub> equivalent* emissions related to agriculture against other sectors.
2. Compare the data from two different sources and provide some key metrics about the differences.

**There are several gases that are generally considered as "greenhouse", each with different magnitude of contribution towards global warming. The common method to normalize the numbers is to multiply the values with a constant that is related to that specific gas, resulting in an equivalent of emitted CO<sub>2</sub>. The multiplication factors that are used in both datasets were taken from the Fifth Assessment report by Intergovernmental Panel on Climate Change (further abbreviated as IPCC).*

### 3 Exploring the data
First I had to understand what is the structure of the data and how the comparison of the two different datasets would look like.

The data about emissions related to agriculture and food systems are maintained by the Food and Agriculture Organization of the United Nations and can be downloaded from their [website](https://www.fao.org/faostat/en/#data/GT) (further abbreviated as FAO). The FAO dataset contains not only agrifood systems data but the emissions data of other sectors that some countries have to report to the IPCC as well.

The second organisation that gathers data about greenhouse gas emissions is Climate Watch. In my simplified scenario, this is the organisation with more precise numbers about emissions. The data can be downloaded either directly from their [website](https://www.climatewatchdata.org/data-explorer/) or accessed via [API](https://www.climatewatchdata.org/api/v1/data/historical_emissions).

In order to get a better understanding of the data and how both datasets fit together, I downloaded sample data for Canada's 2020 emissions from both portals and put them together in a [Google Sheets file](https://docs.google.com/spreadsheets/d/1ZcKa8KzINZwqKoVcgZBC2XNQTrSQdNLt5Au4ie9xVlA/edit#gid=0). The results of my findings are in the following two subsections.

In both cases, I was interested only in downloading data for CO<sub>2</sub> equivalents of emitted gases.

***FAO data***
The data in raw format was not very user friendly. There was no indication about what are the aggregated numbers and what are individual categories. Following along with a table that is attached to the [methodological note of FAOSTAT's Emissions Totals Domain](https://fenixservices.fao.org/faostat/static/documents/GT/GT_e.pdf) I was able to map individual lower level categories to higher level IPCC categories. I have therefore introduced the grading system and assigned appropriate level to each item in a dimension table I have made from selecting distinct values of *Item* and *Item Code*.

| ![Mapping of FAO categories to IPCC sectors](/assets/fao_categories_mapping_to_ipcc.png "Mapping of FAO categories to IPCC sectors") |
| --- |
| *Mapping of FAO categories to IPCC sectors (source - FAOSTAT)* |

Key findings:
1. Some categories might be missing (there was no entry for *Rice Cultivation* emissions - probably because of Canada's lack of rice fields).
2. Emissions for *Food Retail* category was present only in aggregated form (all gases together) but according to the mapping table, some elements were part of the Energy IPCC sector (CO<sub>2</sub>, CH<sub>4</sub>, N<sub>2</sub>O) and some were part of Industrial Processes IPCC sector (F-gases). Therefore additional download of the *Food Retail* category with all the elements was necessary. Additional modifications were to be made in the following ETL process.
3. Level 2 items summed up only for Agriculture and LULUCF IPCC categories. In order to allow the drill-down feature while still displaying the totals, I had to add three new items (*Energy emissions not related to Agriculture*, *IPPU emissions not related to Agriculture*, *Waste emissions not related to Agriculture*) that were to be calculated in the following ETL process.

***Climate Watch data***
The Climate Watch data contained only the values for similar categories as IPCC categories in FAO dataset and more detailed values for Energy sector. Therefore it was ready to be used without any additional changes.

### 4 Ingesting data
ETL Python scripts for ingesting both FAO and Climate Watch datasets were deployed as Google Functions. I had some issues with running the scripts with the basic 256 MiB memory allocation so I had to increase the allocation to 512 MiB (probably becasue of the `pandas` library). 

| ![Overview of the Google Functions for ETL process](/assets/google_functions.png "Overview of the Google Functions for ETL process") |
| --- |
| *Overview of the Google Functions for ETL process* |

***FAO data***
Unfortunately, there is no API to access the data automatically and therefore it is necessary to download the data manually. But the rest of the ETL process was automated. I have created a Google Cloud Function that is triggered by file uploaded to a specific Google Cloud Storage bucket.

*Key sections of the ETL script here*

***Climate Watch data***
Google Cloud Function with parameters that are used in API calls to Climate Watch portal (`start_year`, `end_year`, `regions`). The function transforms the data and uploads them to the Google BigQuery dataset.

### 5 Creating models
Data models created using dbt cloud.

There are references to the [mapping Google Sheets file](https://docs.google.com/spreadsheets/d/1ZcKa8KzINZwqKoVcgZBC2XNQTrSQdNLt5Au4ie9xVlA/edit#gid=0) (items mapping sheet) in this section.

In order to save the compute resources, the initial testing of the models was done on a small amount of data. The testing dataset consisted of three countries - Canada (`CAN`), Nepal (`NPL`) and Guinea (`GIN`). The period covered was 2016 to 2020.

*SQL techniques used: CTEs, window functions (for calculating running total, moving average, delta values - YOY, rank), various joins, unions*

***Data quality models***
First set of models were designed to test the quality of the data and some asumptions made in earlier part of the project. The idea behind testing the data quality is to verify, that the values of higher granularity items (I call these *level 2* items in this project) match the items of lower granularity and that the dataset is complete and both levels can be used interchangebly (i.e. some calculations can be based on level 1 items while others on level 2 items). Discovery of any methodological or process errors made by any of the data collectors (FAOSTAT and Climate Watch) is NOT the purpose of this testing. Therefore if some higher granularity items were not included in calculating the lower granularity items by accident, it won't be discovered.

Data from the FAO dataset are possible to view from two different angles. First one is the IPCC point of view, where sum of the detailed level 2 items (columns `J:K` of the mapping Sheets file) should match the related level 1 items. Second one is the FAO point of view, which has different level 1 categories (columns `N:O` of the mapping Sheets file) that are composed of only some of the level 2 items.

Climate Watch dataset has entries that can be linked only to the FAO's IPCC categories (columns `D:E` of the mapping Sheets file), therefore the data quality models covers only one dimension.

Results of all the data quality tests were combined in one master table. Only one of the tests was showing errors.

| ![Results of all data quality tests](/assets/data_quality_master_result.png "Table that includes results of all data quality tests.") |
| --- |
| *Results of all data quality tests.* |

However, the errors were pretty insignificant and I didn't catch any calculation error (I tested one random example and the numbers were correct).

| ![Errors of fao_fao_test_1 model.](/assets/fao_fao_test_1_result.png "Errors of fao_fao_test_1 model.") |
| --- |
| *Errors of fao_fao_test_1 model.* |

***Core models***
Models that aim to compare the FAO data and Climate Watch data.

### 6 Visualizing the findings
