# Airflow_project
- This project is what I created a small business intelligence system for a start-up. I implemented this whole infrastructure and created 100+ interactive Tableau dashboards with my hand. Therefore, I believe that this infrastructure is valuable for you to reference in your team or a project of your company.
- Airflow can hide sensitive information, e,g., ID, password, connections, in admin site, so pipeline developers are no longer to need the information and the information security can be protected.


## Infrastructure
![flow chart](https://github.com/Auphie/Airflow_project/blob/main/Airflow_proj.png)


## Data sources
- AWS Aurora MySQL 5.7
- Google BigQuery
- Google Analytics
- Google Ads
- Google Sheets
- Elasticsearch (but I would not provide sensitive code here)
- Tableau Online (use Tabcmd to retrieve data)


## Airflow structure
* dags: Managed by scheduled time/frequency of pipelines.
* subdags: Each pipeline has its specific purpose by name.
* bridge.py: An internal API to manage connection and operations with outside data sources.
