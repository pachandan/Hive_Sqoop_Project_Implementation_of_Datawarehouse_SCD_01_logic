# Hive_Sqoop_Project_Implementation_of_Datawarehouse_SCD_01_logic

Client Requirement:

Daily a file will be coming from the client side about the customer purchase data of file type CSV
There will be new records every day and there might also be old records that need to be updated
Client requires SCD TYPE 01 logic to be in the warehouse

Also at end of the processing of each day there data need to be reconciled

Data ingestion

Data is loaded into MYSQL DBMS using command prompt loading
The data after some pre-processing then ingested to HDFS using sqoop
Date Summarisation and Warehousing
Hive is used to manage the Warehousing part

Implemented SCD TYPE 01 logic:

Implemented Partitioning on Year & Month for fast retrieval




Validation:
Once the pipeline is completed the data is at last checked with input records for the count

After every successful operation or failure the log is generated and can be seen for the report and analysis




Script:
Entire warehousing solution is automated using bash scripts

All credentails,output direotries, DBMS details are made dynamic using parmeter file and credential files




Execution
Project Work Flow
![Project_workflow](https://user-images.githubusercontent.com/107995832/178541300-b3cb1e1e-d798-4be5-8a72-cea7714dba22.jpeg)
