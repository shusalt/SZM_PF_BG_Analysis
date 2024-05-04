-- Apache Doris表设计
create database szm;


-- dwd_fact_szt_in_out_detail_doris(地铁进出站总表)
drop table if exists szm.dwd_fact_szt_in_out_detail_doris;
create table szm.dwd_fact_szt_in_out_detail_doris(
	`deal_date` varchar(255) comment '刷卡日期',
	`close_date` varchar(255) comment '关闭时间',
	`card_no` varchar(255) comment '卡号',
	`deal_type` varchar(255) comment '出入站类型："地铁入站"、"地铁出站"',
	`company_name` varchar(255) comment '地铁线名称',
	`car_no` varchar(255) comment '地铁列车号',
	`station` varchar(255) comment '地铁站',
	`conn_mark` varchar(255) comment '连续标记',
	`dt` varchar(255) comment 'hive分区字段',
	`deal_value` decimal(16, 2) comment '交易价值',
	`deal_money` decimal(16, 2) comment '实收金额',
	`equ_no` varchar(255) comment '闸机编号'
)
COMMENT '地铁进出站总表'
ENGINE=OLAP
DUPLICATE KEY(`deal_date`,`close_date`,`card_no`,`deal_type`,`company_name`,`car_no`,`station`,`conn_mark`, `dt`)
DISTRIBUTED BY HASH(`card_no`) BUCKETS 1
PROPERTIES ('replication_num'='1');


-- dwd_fact_szt_in_detail_doris(地铁进站事实表)
drop table if exists szm.dwd_fact_szt_in_detail_doris;
create table szm.dwd_fact_szt_in_detail_doris(
	`deal_date` varchar(255) comment "刷卡日期",
	`card_no` varchar(255) comment "卡号",
	`company_name` varchar(255) comment "地铁线名称",
	`car_no` varchar(255) comment "地铁列车号",
	`station` varchar(255) comment "地铁站",
	`dt` varchar(255) comment 'hive分区字段',
	`equ_no` varchar(255) comment "闸机编号"
) 
COMMENT "地铁进站事实表"
ENGINE=OLAP
DUPLICATE KEY(`deal_date`, `card_no`, `company_name`, `car_no`, `station`, `dt`)
DISTRIBUTED BY HASH(card_no) BUCKETS 1
PROPERTIES ('replication_num'='1');



-- dwd_fact_szt_out_detail(地铁出站事实表)
drop table if exists szm.dwd_fact_szt_out_detail_doris;
create table dwd_fact_szt_out_detail_doris(
	`deal_date` varchar(255) comment "刷卡日期",
	`close_date` varchar(255) comment "关闭时间",
	`card_no` varchar(255) comment "卡号",
	`company_name` varchar(255) comment "地铁线名称",
	`car_no` varchar(255) comment "地铁列车号",
	`station` varchar(255) comment "地铁站",
	`conn_mark` varchar(255) comment "连续标记",
	`dt` varchar(255) comment 'hive分区字段',
	`deal_value` decimal(16, 2) comment "交易价值",
	`deal_money` decimal(16, 2) comment "实收金额",
	`equ_no` varchar(255) comment "闸机编号"
) 
COMMENT "地铁出站事实表"
ENGINE=OLAP
DUPLICATE KEY(`deal_date`, `close_date`, `card_no`, `company_name`, `car_no`, `station`, `conn_mark`, `dt`)
DISTRIBUTED BY HASH(card_no) BUCKETS 1
PROPERTIES ('replication_num'='1');




-- doris stream load
-- dwd_fact_szt_in_out_detail_doris 数据装载
curl --location-trusted -u root: \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns:deal_date,close_date,card_no,deal_value,deal_type,company_name,car_no,station,conn_mark,deal_money,equ_no,dt" \
    -T part-00000-bb3ffe26-b95d-4bf5-8758-3c0d1eecbc3a-c000.csv \
    -XPUT http://172.20.10.3:8070/api/szm/dwd_fact_szt_in_out_detail_doris/_stream_load

curl --location-trusted -u root: \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns:deal_date,card_no,company_name,car_no,station,equ_no,dt" \
    -T part-00000-089eba11-ed8f-41e7-b9a6-15ac7b9131e2-c000.csv \
    -XPUT http://172.20.10.3:8070/api/szm/dwd_fact_szt_in_detail_doris/_stream_load

curl --location-trusted -u root: \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns:deal_date,close_date,card_no,deal_value,company_name,car_no,station,conn_mark,deal_money,equ_no,dt" \
    -T part-00000-a27c3213-789b-4326-9100-57c4a9ce3f1d-c000.csv \
    -XPUT http://172.20.10.3:8070/api/szm/dwd_fact_szt_out_detail_doris/_stream_load