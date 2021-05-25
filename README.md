# Coding Challenge:
This repo is for the coding challenge to a Parse the Json and create a CSV file 

I have used databricks notebook to code this. Input JSON file has been uploaded to databricks file system and read from there. Output folder has final CSV file.

# Concept Ideation:
We have a model and making predictions, how would you store predictions in cloud, monitor the performance, set access policies, etc.

Assumptions and explaination:
1. since it is a predictions data it will be small data which can be stored in simple RDS. I dont think we need warehousing service hear to store the data. But if you want end user not to login to AWS we have simply provide an API to access the data by storing data in redshift.
2. This data can be consumed by other flows to make some forecasting and visualization
3. AWS S3 has a processed data in parquet compressed format written by either EMR/ Glue jobs
4. All the jobs are monitored by AWS cloud watch to know more about respond to system-wide performance changes, optimize resource utilization, and get a unified view of operational health
5. 

## Block diagra![Untitled Diagram](https://user-images.githubusercontent.com/26443357/119561254-f1779700-bd72-11eb-95cd-5b5577410f9f.jpg)
m


