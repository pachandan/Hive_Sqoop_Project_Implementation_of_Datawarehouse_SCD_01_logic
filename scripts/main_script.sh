#!/bin/bash

#bringing the param file:
. /home/saif/cohort_f10_f11/project1/env/sqp.prm

#log file detials
LOG_DIR=/home/saif/cohort_f10_f11/project1/logs
FILE_NAME=`basename $0`
DT=`date '+%Y%m%d_%H:%M:%S'`
LOG_FILE=$LOG_DIR/${FILE_NAME}_${DT}.log

#command to create table in mysql
##validating mysql commands

mysql --user="root" --password="Welcome@123" --database="project1"

--execute="create table if not exists customer_p_day_1(
custid int,
username varchar(30),
quote_count varchar(30),
ip varchar(30),
entry_time varchar(30),
prp_1 varchar(30),
prp_2 varchar(30),
prp_3 varchar(30),
ms varchar(30),
http_type varchar(50),
purchase_category varchar(30),
total_count varchar(30),
purchase_sub_category varchar(30),
http_info varchar(50),
status_code int,
current_dt_stmp timestamp,
entry_year INT,
entry_month INT);"

#movind daata form local file to mysql
file_lt=`ls /home/saif/Desktop/Project_1/datasets -tp | grep -v /$ | head -1`
echo $file_lt
mysql --local-infile=1 -uroot -pWelcome@123 -e "set global local_infile=1 ;
truncate table project1.customer_p;

LOAD DATA LOCAL INFILE '/home/saif/cohort_f10_f11/datasets/Day_1.csv' 
INTO TABLE customer_p_day_1
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
(custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category, total_count, purchase_sub_category, http_info, status_code,@entry_year,@entry_month)
SET entry_year = DATE_FORMAT(STR_TO_DATE(entry_time,'%d/%b/%Y'),'%Y'),entry_month = DATE_FORMAT(STR_TO_DATE(entry_time,'%d/%b/%Y'),'%m');"

##validating mysql commands
if [ $? -eq 0 ]
then echo "mysql successfully executed at ${DT}" >> ${LOG_FILE}
else echo "mysql commands failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


OP_DIR=/user/saif/HFS/Output/Project1

#Dir Checking whether it exists or not:
hdfs dfs -test -d ${OP_DIR}

#Validation for Dir existence:
if [ $? -eq 0 ]
then
        hdfs dfs -rm -r ${OP_DIR}
        echo "Dir Deleted Successfully"
fi

#scoop import form mysql to hdfs 

sqoop import \
--connect jdbc:mysql://localhost:3306/project1?useSSL=False \
--username root \
--password-file file:///home/saif/cohort_f10_f11/datasets/sqoop.pwd \
--query 'select * from customer_p_day_1 where $CONDITIONS' \
--delete-target-dir --target-dir  /user/saif/HFS/Input/inc_imports_customer \
--split-by custid \
--m 1 




if [ $? -eq 0 ]
then echo "sqoop loading  completed ${DT}" >> ${LOG_FILE}
else echo "sqoop import failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi

#hdfs to hive table
hive -f '/home/saif/cohort_f10_f11/project1/scripts/hive.hql'
hive -f '/home/saif/cohort_f10_f11/project1/scripts/hive_table1.hql'
hive -f '/home/saif/cohort_f10_f11/project1/scripts/hive_table2.hql'
hive -f '/home/saif/cohort_f10_f11/project1/scripts/hive_table3.hql'


if [ $? -eq 0 ]
then echo "hive scd successfully executed at ${DT}" >> ${LOG_FILE}
else echo "hive scd job failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi


sqoop export --connect jdbc:mysql://${HOST}:${PORT_NO}/${DB_NAME}?useSSL=False \
-username ${USERNAME} --password-file ${PASSWORD_FILE} \
--table reconcilation_tbl \
--export-dir /user/hive/warehouse/project_1.db/cust_stagging \
--input-fields-terminated-by ',' 

if [ $? -eq 0 ]
then echo "sqoop exp job successfully executed at ${DT}" >> ${LOG_FILE}
else echo "sqoop exp job failed  at ${DT} " >> ${LOG_FILE}
exit 1
fi