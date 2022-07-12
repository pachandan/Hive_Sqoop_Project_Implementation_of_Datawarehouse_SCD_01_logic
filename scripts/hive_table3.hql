set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.enforce.bucketing = true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency=true;

create table if not exists project1.hv_ext_customer_p
(
custid int,
username string,
quote_count string,
ip string,
entry_time string,
prp_1 string,
prp_2 string,
prp_3 string,
ms string,
http_type string,
purchase_category string,
total_count string,
purchase_sub_category string,
http_info string,
status_code string,
current_dt_stmp string
)
partitioned by (entry_year string,entry_month string)
row format delimited fields terminated by ','
stored as orc
tblproperties("transactional"="true");

insert overwrite table Project1.hv_ext_customer_p(select st.custid,st.username,st.quote_count,st.ip,st.entry_time,st.prp_1,st.prp_2,st.prp_3,st.ms,
st.http_type,st.purchase_category,st.total_count,
st.purchase_sub_category,st.http_info,st.status_code,st.current_dt_stmp,st.entry_year,st.entry_month from Project1.hv_intr_customer_p st
full outer join Project1.hv_ext_customer_p tgt on tgt.custid=st.custid
where st.custid is not null
union
select tgt1.custid,tgt1.username,tgt1.quote_count,tgt1.ip,tgt1.entry_time,tgt1.prp_1,tgt1.prp_2,tgt1.prp_3,tgt1.ms,
tgt1.http_type,tgt1.purchase_category,tgt1.total_count,
tgt1.purchase_sub_category,tgt1.http_info,tgt1.status_code,tgt1.current_dt_stmp,tgt1.entry_year,tgt1.entry_month from Project1.hv_intr_customer_p st1
full outer join Project1.hv_ext_customer_p tgt1 on tgt1.custid=st1.custid
where st1.custid is null);

