# Airflow_project
- This project is what I created a small business intelligence system for a start-up. I implemented this whole infrastructure and created 100+ interactive Tableau dashboards with my hand. Therefore, this data infrastructure is valuable for you if your company or team does not have sufficient resources.
- You can hide sensitive information, e,g., ID, password, connections, via the Airflow admin site. Therefore, without any sensitive information in pipeline codes, the company's information security is then protected.


## Infrastructure
![flow chart](https://github.com/Auphie/Airflow_project/blob/main/architecture.png)


## Data sources
- AWS Aurora MySQL 5.7
- Google BigQuery
- Google Analytics
- Google Ads
- Google Sheets
- Elasticsearch (but I would not provide sensitive code here)
- Tableau Online (use Tabcmd to retrieve data)


## Airflow structure
* dags folder: Managed by scheduled time/frequency of pipelines.
* subdags floder: Each pipeline has its specific purpose from its file name, so the pipelines are easier to maintain.
* bridge.py: An internal API to connect and operate between data sources and data warehouse.
