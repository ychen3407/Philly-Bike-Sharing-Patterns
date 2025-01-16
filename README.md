# Philly-Bike-Sharing-Patterns DE Project :bike:
Philadelphia is recognized as one of the most bike-friendly cities in the United States. The Indego bike share program maintains hundreds of self-serve stations across the city, where individuals can easily access bikes for quick trips or purchase longer-term passes.

<img src="images/IndegoBikes.jpg?raw=true">

This project focuses on building a data pipeline to analyze bike share patterns in Philadelphia.

### API Data Sources:
* Trip Data ([Indego Ride](https://www.rideindego.com/about/data/))
* Station Status, Station Info ([GBFS](https://gbfs.bcycle.com/bcycle_indego/gbfs.json))
* Weather ([Open Meteo](https://open-meteo.com/))

These data sources provides comprehensive information for us to explore the following questions:
1. Are stations utilized efficiently? Are there any underutilized or overutilized stations?
2. How does bikes demand vary over different time of a day and different day of a week?
3. Does weather, such as temperature and rainfall/snow, affect the bike usage? If so, how?

The data comes from both real-time sources (e.g., station status) and batch sources, which are handled separately. The pipeline architecture is illustrated below: 

<img src="images/Architecture.png?raw=true">

### Tools Used:
- Airflow: Workflow orchestration
- Docker
- Terraform: Architecture management
- Kafka, PySpark: Real-time processing
- Cloud Storage: Data lake
- BigQuery: Data warehouse
- dbt: Data transformation and modeling tool
- Looker Studio: Data visualization

### Workflow Overview:
I used Airflow to schedule Python jobs for data ingestion at different frequencies, loading raw data into Cloud Storage and staging data into BigQuery:

- Station status: Data was ingested every five minutes, treated as near real-time data. (Using Kafka, PySpark)
- Station info and weather: Data was ingested daily. (Using Python)
- Trip data: Ingested only once, as Indego releases quarterly CSV files. (Using Python)

DBT was used for data modeling in BigQuery. For this project, I chose a star schema to structure the data.

<img src="images/StarSchema.png?raw=true">

Finally, Looker Studio was utilized to create visualizations for both historical trends and real-time station status.

The historical trend dashboard looks like [this](https://lookerstudio.google.com/reporting/326e0609-09c1-4e85-ae91-530042fad004):

<img src="images/DashboardHistorical.png?raw=true">

Real-time Dashboard looks like this:

<img src="images/DashboardRealTime.jpg?raw=true">

### Some Insights
From the analysis and dashboard, I had found that 
* Bike demand fluctuates significantly throughout the day, with notable peaks during rush hours (i.e., 8 AM and 6 PM). During these periods, there are more trips, but bike availability tends to be lower due to higher demand
* Some stations experience consistently high demand, such as Station 3010 (15th & Spruce) and Station 3032 (23rd & South). In contrast, stations located in more peripheral or less densely populated areas tend to be underutilized.
* Note: We are in the process of accumulating more data to fully understand the impact of weather variables (temperature, rainfall, and snow) on bike usage. 
