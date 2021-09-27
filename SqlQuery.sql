

create external table mercedesDB(rowkey string, model string, year string, price float, transmission string, mileage double, fuelType string, tax double, mpg float, engineSize float) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 'cf:model, cf:year, cf:price#b, cf:transmission, cf:mileage#b, cf:fuelType, cf:tax#b, cf:mpg#b, cf:engineSize#b') TBLPROPERTIES ('hbase.table.name' = 'mercedesDB');






select count(model), year from mercedesDB group by model

create view carcountview as select model, count(*) as number from mercedesDB group by model;

select model, count(*) as number from mercedesDB LIMIT 5

select fueltype, count(*) as number from mercedesDB group by fueltype;


select model, count(*) as number from mercedesDB where price > 50000 group by model;

SELECT * FROM mercedesDB WHERE model = 'G Class' AND mileage > 100000