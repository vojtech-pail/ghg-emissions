# Greenhouse gas emissions data comparison
This is an analytical project which aims to provide comparison of two different sources of data about greenhouse gas emissions. 

## Problem definition
Even though this is just a test project let's build some real foundations for its justification.

The scientists have already discovered in several studies that if the trend of global warming continues, there will be pretty bad consequences for human kind. Since we haven't invented any tools or mechanisms to cool down the planet, the only possible action we can take is to lower the amount of emitted greenhouse gases that contribute towards the warming of the planet.

One of the areas that might be ideal for optimisation is the food consumption. Usually it's almost no problem to get the products we want in a certain moment even though they were produced on the other side of the planet. The food availability causes some unnecessary greenhouse gases emission that are either related to the transportation of the goods or by over-production.

## The project goal
Now the fun part begins (with a little made up story).

Before making any changes like prohibiting international trade, we first want to see the composition of emissions related to different sectors in order to understand the significance of the food production. There is also suspicion that countries do not report their emissions correctly in order to look better compared to other countries. But luckily there is one organisation that have more precise numbers of emitted greenhouse gases so our goal is also to see how off the countries' reports are.

The goals are:
1. **Visualize the proportion of emissions related to agriculture against other sectors.**
2. **Compare the data from two different sources.**

## Exploring available data
What can be extracted. Are there some areas that can be compared?
Testing the availability on datapoints for Canada. Add the Google Sheets report.


### FAO portal


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
