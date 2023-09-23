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

**There are several gases that are generally considered as "greenhouse", each with different magnitude of contribution towards global warming. The common method to normalize the numbers is to multiply the values with a constant that is related to that specific gas, resulting in an equivalent of emitted CO2. The multiplication factors that are used in both datasets were taken from the Fifth Assessment report by Intergovernmental Panel on Climate Change (further abbreviated as IPCC).*

## 3 Exploring the data
First I had to understand what is the structure of the data and how the comparison of the two different datasets would look like.

The data about emissions related to agriculture and food systems are maintained by the Food and Agriculture Organization of the United Nations and can be downloaded from their [website](https://www.fao.org/faostat/en/#data/GT) (further abbreviated as FAO). The FAO dataset contains not only agrifood systems data but the emissions data of other sectors that some countries have to report to the IPCC as well.

The second organisation that gathers data about greenhouse gas emissions is Climate Watch. In my simplified scenario, this is the organisation with more precise numbers about emissions. The data can be downloaded either directly from their [website](https://www.climatewatchdata.org/data-explorer/) or accessed via API.

In order to get a better understanding of the data and how both datasets fit together, I downloaded sample data for Canada's 2020 emissions from both portals and put them together in a [Google Sheets file](https://docs.google.com/spreadsheets/d/1ZcKa8KzINZwqKoVcgZBC2XNQTrSQdNLt5Au4ie9xVlA/edit#gid=0). The results of my findings are in the following two subsections.

In both cases, I was interested only in downloading data for CO2 equivalents of emitted gases.

### 3.1 FAO data
The data downloaded 
![Mapping of FAO categories to IPCC sectors!](/assets/fao_categories_mapping_to_ipcc.png "Mapping of FAO categories to IPCC sectors")
*Mapping of FAO categories to IPCC sectors ()*

### 3.2 Climate Watch data

## 4 Ingesting data

### FAO data
Manula download + cloud storage triggering google function that transforms the data and uploads them to the Google BigQuery dataset.

### Climate Watch data
Google Cloud Function with parameters that are used in API calls to Climate Watch portal (`start_year`, `end_year`, `regions`). The function transforms the data and uploads them to the Google BigQuery dataset.

## 5 Creating models
Using dbt cloud.

### Data quality models
Checking reliability and completeness.
Summarizing findings in one master model.
Result - tests don't pass.

### Core models
Models that aim to compare the FAO data and Climate Watch data.

## 6 Visualizing the findings
