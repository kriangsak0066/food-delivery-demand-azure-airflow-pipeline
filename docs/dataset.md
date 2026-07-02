# Dataset



## Source



Food Delivery Order History Data



Source URL:

https://www.kaggle.com/datasets/sujalsuthar/food-delivery-order-history-data



## Description



This dataset contains food delivery order records. It will be used to analyze order demand, restaurant performance, delivery SLA, customer behavior, and demand patterns by time.



## Raw Data Storage



Local raw files are stored under:



`data/raw/food\_delivery\_orders/`



In the production-like pipeline, raw files will be uploaded to Azure Blob Storage using this path pattern:



`raw/food\_delivery/orders/load\_date=YYYY-MM-DD/orders.csv`



## Planned Usage



The dataset will be loaded into Azure SQL Database using the following layers:



`raw file -> staging table -> warehouse tables -> mart tables -> Power BI`



## Notes



The raw dataset should not be modified directly. Any cleaning or transformation should happen in staging, warehouse, or mart SQL scripts.

