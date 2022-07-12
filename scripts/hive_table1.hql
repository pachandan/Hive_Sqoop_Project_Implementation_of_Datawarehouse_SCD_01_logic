create table if not exists project1.hv_customer_p
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
entry_year  int,
entry_month int
)
row format delimited fields terminated by ',';

load data inpath '/user/saif/HFS/Input/inc_imports_customer/' overwrite into table hv_customer_p;



