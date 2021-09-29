--5.1	Creation d'une table simple avec Hive
 CREATE EXTERNAL TABLE IF NOT EXISTS sales_raw (
 product_id String,
 time_id String,
 customer_id String,
 promotion_id String,
 store_id String,
 store_sales String,
 store_cost String,
 unit_sales String)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ';'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE
 LOCATION '/user/raj_ops/dmo/hive/sales.raw'
 
 
 
 
--5.2	Création d'une table partitionned

 CREATE EXTERNAL TABLE IF NOT EXISTS sales_part (
 product_id String,
 time_id String,
 customer_id String,
 promotion_id String,
 store_sales String,
 store_cost String,
 unit_sales String)
 Partitioned by (store_id String)
 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ';'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE
 LOCATION '/user/raj_ops/dmo/hive/partitionned.sales'
 
--5.3	 Insert table dans une table de partition 
 INSERT INTO sales_part(  
 select 
 product_id ,
 time_id ,
 customer_id ,
 promotion_id,
 store_sales ,
 store_cost ,
 unit_sales ,
 store_id from sales_raw
 )
--5.4	  Selectionner des tables 
 select count(*) from sales_raw where store_id='1' ;
 ==>le temps d'exécution est du 6 s
 
 select count(*) from sales_partition where store_id='1' ;
 ==>Le temps d'exécusion est de 2 s

--5.5	Création d'une table ORC

CREATE EXTERNAL TABLE IF NOT EXISTS sales_orc (
 product_id String,
 time_id String,
 customer_id String,
 promotion_id String,
 store_id String,
 store_sales String,
 store_cost String,
 unit_sales String)
 STORED AS ORC
 
 LOCATION '/user/raj_ops/sales.orc'

5.6	Insertion dans une table ORC
  INSERT INTO sales_orc(  
 select 
 product_id ,
 time_id ,
 customer_id ,
 promotion_id,
 store_id,
 store_sales ,
 store_cost ,
 unit_sales
 from sales_raw
 )
 
--5.7	Création d'une table de partition sous format ORC
CREATE EXTERNAL TABLE IF NOT EXISTS sales_partition_orc (
 product_id String,
 time_id String,
 customer_id String,
 promotion_id String,
 store_sales String,
 store_cost String,
 unit_sales String)
 Partitioned by (store_id String)
 STORED AS ORC
 LOCATION '/user/raj_ops/partitionned.sales.orc'
--5.8	 Insertion dans la table sales_partition_orc
  INSERT INTO sales_partition_orc(  
 select 
 product_id ,
 time_id ,
 customer_id ,
 promotion_id,
 store_sales ,
 store_cost ,
 unit_sales ,
 store_id from sales_raw
 )
--5.9	Création d'une table dans le shéma est identique à une table déjà existante
 CREAT TABLE  ......like
 
5.10	 Création une table stores_raw
 
CREATE EXTERNAL TABLE IF NOT EXISTS stores_raw (
store_id String,
store_type String,
region_id String,
store_name String,
store_number String,
store_street_address String,
store_city String,
store_state String,
store_postal_code String,
store_country String,
store_manager String,
store_phone String,
store_fax String,
first_opened_date String,
last_remodel_date String,
store_sqft String,
grocery_sqft String,
frozen_sqft String,
meat_sqft String,
coffee_bar String,
video_store String,
salad_bar String,
prepared_food String,
florist String)

 ROW FORMAT DELIMITED
 FIELDS TERMINATED BY ';'
 LINES TERMINATED BY '\n'
 STORED AS TEXTFILE
 LOCATION '/user/raj_ops/stores.raw'
 
5.11	Faire une joiture entre sales_raw et stores_raw
select* from sales_raw join stores_raw on (sales_raw.store_id = stores_raw.store_id) limit 5;
