create table if not exists project1.hv_intr_customer_p
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
current_dt_stmp string,
entry_year string,
entry_month string
)
row format delimited fields terminated by ',';

insert overwrite table hv_intr_customer_p select custid,
username,quote_count,ip,entry_time,prp_1,prp_2,prp_3,ms,http_type,purchase_category,total_count,
purchase_sub_category  ,http_info  ,status_code  ,current_dt_stmp,entry_year,entry_month from hv_customer_p;

