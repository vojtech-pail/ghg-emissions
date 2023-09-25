# Greenhouse gas emissions data comparison
This is an end-to-end data engineering and data analytical project which aims to provide comparison of two different data sources about greenhouse gas emissions and visualize the greenhouse gas contributions of different sectors and agrifood systems in particular.

*DISCLAIMER: In order to focus on the data engineering and analytical tasks, I disregard some technical details about the datasets and make some intentional assumptions about them that might not be correct. Please use the results with caution.*

## 1 The context
Even though this is just a test project let's build some real foundations for its justification.

The scientists have already discovered in several studies that if the trend of global warming continues, there will be pretty bad consequences for human kind. Since we haven't invented any tools or mechanisms to cool down the planet, the only possible action we can take is to lower the amount of emitted greenhouse gases that contribute towards the warming of the planet.

One of the areas that might be ideal for optimisation is the food consumption. Usually it's almost no problem to get the products we want at a certain moment even though they were produced on the other side of the planet. The food availability causes some unnecessary greenhouse gases emission that are either related to the transportation of the goods or by over-production.

## 2 The project goal
Now the fun part begins. As mentioned in the disclaimer, there are some assumptions about the datasets and due to simplification some information in this section is made up.

Before making any changes like prohibiting international trade, we first want to see the composition of emissions related to different sectors in order to understand the significance of the food production. There is also suspicion that countries do not report their emissions correctly in order to look better compared to other countries. But luckily there is one organisation that have more precise numbers of emitted greenhouse gases so our goal is also to see how off the countries' reports are.

The goals:
1. Visualize the proportion of CO<sub>2</sub> equivalent* emissions related to agriculture against other sectors.
2. Compare the data from two different sources and provide some key metrics about the differences.

**There are several gases that are generally considered as "greenhouse", each with different magnitude of contribution towards global warming. The common method to normalize the numbers is to multiply the values with a constant that is related to that specific gas, resulting in an equivalent of emitted CO<sub>2</sub>. The multiplication factors that are used in both datasets were taken from the Fifth Assessment report by Intergovernmental Panel on Climate Change (further abbreviated as IPCC).*

## 3 Exploring the data
First I had to understand what is the structure of the data and how the comparison of the two different datasets would look like.

The data about emissions related to agriculture and food systems are maintained by the Food and Agriculture Organization of the United Nations and can be downloaded from their [website](https://www.fao.org/faostat/en/#data/GT) (further abbreviated as FAO). The FAO dataset contains not only agrifood systems data but the emissions data of other sectors that some countries have to report to the IPCC as well.

The second organisation that gathers data about greenhouse gas emissions is Climate Watch. In my simplified scenario, this is the organisation with more precise numbers about emissions. The data can be downloaded either directly from their [website](https://www.climatewatchdata.org/data-explorer/) or accessed via API.

In order to get a better understanding of the data and how both datasets fit together, I downloaded sample data for Canada's 2020 emissions from both portals and put them together in a [Google Sheets file](https://docs.google.com/spreadsheets/d/1ZcKa8KzINZwqKoVcgZBC2XNQTrSQdNLt5Au4ie9xVlA/edit#gid=0). The results of my findings are in the following two subsections.

In both cases, I was interested only in downloading data for CO<sub>2</sub> equivalents of emitted gases.

### 3.1 FAO data
The data in raw format was not very user friendly. There was no indication about what are the aggregated numbers and what are individual categories. Following along with a table that is attached to the [methodological note of FAOSTAT's Emissions Totals Domain](https://fenixservices.fao.org/faostat/static/documents/GT/GT_e.pdf) I was able to map individual lower level categories to higher level IPCC categories. I have therefore introduced the grading system and assigned appropriate level to each item in a dimension table I have made from selecting distinct values of *Item* and *Item Code*.

| ![Mapping of FAO categories to IPCC sectors](/assets/fao_categories_mapping_to_ipcc.png "Mapping of FAO categories to IPCC sectors") |
| --- |
| *Mapping of FAO categories to IPCC sectors (source - FAOSTAT)* |

**Some of the key findings:**
1. Some categories might be missing (there was no entry for *Rice Cultivation* emissions - probably because of Canada's lack of rice fields).
2. Emissions for *Food Retail* category was present only in aggregated form (all gases together) but according to the mapping table, some elements were part of the Energy IPCC sector (CO<sub>2</sub>, CH<sub>4</sub>, N<sub>2</sub>O) and some were part of Industrial Processes IPCC sector (F-gases). Therefore additional download of the *Food Retail* category with all the elements was necessary. Additional modifications were to be made in the following ETL process.
3. Level 2 items summed up only for Agriculture and LULUCF IPCC categories. In order to allow the drill-down feature while still displaying the totals, I had to add three new items (*Energy emissions not related to Agriculture*, *IPPU emissions not related to Agriculture*, *Waste emissions not related to Agriculture*) that were to be calculated in the following ETL process.

### 3.2 Climate Watch data
The Climate Watch data contained only the values for similar categories as IPCC categories in FAO dataset and more detailed values for Energy sector. Therefore it was ready to be used without any additional changes.

## 4 Ingesting data
ETL Python scripts for ingesting both FAO and Climate Watch datasets were deployed as Google Functions. I had some issues with running the scripts with the basic 256 MiB memory allocation so I had to increase the allocation to 512 MiB (probably becasue of the `pandas` library). 

| ![Overview of the Google Functions for ETL process](/assets/google_functions.png "Overview of the Google Functions for ETL process") |
| --- |
| *Overview of the Google Functions for ETL process* |

### 4.1 FAO data
Unfortunately, there is no API to access the data automatically and therefore it is necessary to download the data manually. But the rest of the ETL process was automated. I have created a Google Cloud Function that is triggered by file uploaded to a specific Google Cloud Storage bucket.

*Key sections of the ETL script here*

### 4.2 Climate Watch data
Google Cloud Function with parameters that are used in API calls to Climate Watch portal (`start_year`, `end_year`, `regions`). The function transforms the data and uploads them to the Google BigQuery dataset.

## 5 Creating models
Data models created using dbt cloud.

There are references to the [mapping Google Sheets file](https://docs.google.com/spreadsheets/d/1ZcKa8KzINZwqKoVcgZBC2XNQTrSQdNLt5Au4ie9xVlA/edit#gid=0) (items mapping sheet) in this section.

In order to save the compute resources, the initial testing of the models was done on a small amount of data. The testing dataset consisted of three countries - Canada (`CAN`), Nepal (`NPL`) and Guinea (`GIN`). The period covered was 2016 to 2020.

### Data quality models
First set of models were designed to test the quality of the data and some asumptions made in earlier part of the project.

Data from the FAO dataset are possible to view from two different angles. First one is the IPCC point of view, where sum of the detailed level 2 items (columns `J:K` of the mapping Sheets file) should match the related level 1 items. Second one is the FAO point of view, which has different level 1 categories (columns `N:O` of the mapping Sheets file) that are composed of only some of the level 2 items.

Climate Watch dataset has entries that can be linked only to the FAO's IPCC categories, therefore the data quality models covers only one dimension.

*Summarizing findings in one master model.*

**FAO data completeness (IPCC dimension) [fao_ipcc_test_1.sql]**
* The purpose of this model is to test the asumption made in the initial data exploring phase described in the finding number 3 of the section 3.1, that level 2 items (columns `J:K` of the mapping Sheets file) add up to level 1 items for *LULUCF* and *Agriculture* IPCC  categories (columns `H:I` of the mapping Sheets file).
* There were discrepancies in *LULUCF* category for Nepal and Guinea entries.
* => I had to go back and add new level 2 item *LULUCF emissions not related to agriculture* to the dimension table and edit the ETL script so it is calculated with the other custom items.

**FAO data completeness (IPCC dimension) [fao_ipcc_test_1_2.sql]**
* This is a revised version of the *fao_ipcc_test_1* model, which includes comparison of level 2 and level 1 items only for agriculture IPCC sector.

**FAO data completeness (IPCC dimension) [fao_ipcc_test_2.sql]**
* The second IPCC data completeness model verifies that the sum of level 1 items (columns `H:I` of the mapping Sheets file) are equal to the level 0 items (total amount of emissions per year and country). 

**FAO data completeness (FAO dimension) [fao_fao_test_1.sql]**
* This model evaluates whether level 2 items (columns `J:K` of the mapping Sheets file) from the FAO dataset are equal to the related level 1 items (columns `N:O` of the mapping Sheets file) of the FAO dimension.


### Core models
Models that aim to compare the FAO data and Climate Watch data.

## 6 Visualizing the findings
