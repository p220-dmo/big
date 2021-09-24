# Spark DataFrame/DataSet

## Introduction
La description des données utilisées pour ses exercices est accessible [ici](https://github.com/Ahmed-Gater/spark-in-practice/blob/master/datasetdescription.md).
La version de Spark pour ces exercices est 2.4.2 et scala 11.  

## Exercices

<details><summary>Exercice 1: Réécrire les exercices de la section SparkCore avec l'API DataFrame ?</summary>
<p>

<details><summary>Solution de l'exercice 2: Charger le fichier des ventes (sales.csv) dans un DataFrame

```java
DataSet<Row> salesAsDF = ...
```

</summary>  
<p>
  
#### Solution: un fichier peut être chargé en laissant l'API inférrer le schéma ou définir son propre schéma avec DataTypes !!!
```java
// Version 1: Charger sans schéma
Dataset<Row> salesAsDFWithoutSchemaInferring = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .load(filePath);

// Version 2: Charger le fichier en définissant un schéma
ArrayList<StructField> fields = new ArrayList<>(
                Arrays.asList(
                        DataTypes.createStructField("PRODUCT_ID", DataTypes.LongType, true),
                        DataTypes.createStructField("TIME_ID", DataTypes.IntegerType, true),
                        DataTypes.createStructField("CUSTOMER_ID", DataTypes.LongType, true),
                        DataTypes.createStructField("PROMOTION_ID", DataTypes.LongType, true),
                        DataTypes.createStructField("STORE_ID", DataTypes.IntegerType, true),
                        DataTypes.createStructField("STORE_SALES", DataTypes.DoubleType, true),
                        DataTypes.createStructField("STORE_COST", DataTypes.DoubleType, true),
                        DataTypes.createStructField("UNIT_SALES", DataTypes.DoubleType, true)
                ));
StructType schema = DataTypes.createStructType(fields);
Dataset<Row> salesAsDFWithSchemaDefined = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(schema)
                .load(filePath);
```

avec les schémas correspondant:

```java
// Printing schema of dataframe loaded by inferring schema
salesAsDFWithoutSchemaInferring.printSchema() ;
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)
 |-- _c5: string (nullable = true)
 |-- _c6: string (nullable = true)
 |-- _c7: string (nullable = true)


// Printing schema of dataframe loaded by defining schema
salesAsDFWithSchemaDefined.printSchema() ;
root
 |-- PRODUCT_ID: long (nullable = true)
 |-- TIME_ID: integer (nullable = true)
 |-- CUSTOMER_ID: long (nullable = true)
 |-- PROMOTION_ID: long (nullable = true)
 |-- STORE_ID: integer (nullable = true)
 |-- STORE_SALES: double (nullable = true)
 |-- STORE_COST: double (nullable = true)
 |-- UNIT_SALES: double (nullable = true)
```
</p>
</details>

<details><summary>Solution de l'exercice 3: charger le fichier des ventes (sales.csv) dans une Dataset<Sale>

```java
Dataset<Sale> as  = ...
```
</summary>  

#### Solution: On peut transformer un Dataset\<Row> à un Dataset\<Sale> en utilisant un encoder ou un aprés mapping du dataframe (si on veut dériver des objets avant d'appliquer l'encoder) !!!

```java
// Version 1: transformer avec un encoder
ArrayList<StructField> fields = new ArrayList<>(
                Arrays.asList(
                        DataTypes.createStructField("productId", DataTypes.LongType, true),
                        DataTypes.createStructField("timeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("customerId", DataTypes.LongType, true),
                        DataTypes.createStructField("promotionId", DataTypes.LongType, true),
                        DataTypes.createStructField("storeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("storeSales", DataTypes.DoubleType, true),
                        DataTypes.createStructField("storeCost", DataTypes.DoubleType, true),
                        DataTypes.createStructField("unitSales", DataTypes.DoubleType, true)
                ));
StructType schema = DataTypes.createStructType(fields);
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(schema)
                .load(filePath);
Encoder<Sale> saleEncoder = Encoders.bean(Sale.class);
Dataset<Sale> as = salesAsDF.as(saleEncoder);
as.printSchema();

// Version 2: transformer avec une Map
ArrayList<StructField> fields = new ArrayList<>(
                Arrays.asList(
                        DataTypes.createStructField("productId", DataTypes.LongType, true),
                        DataTypes.createStructField("timeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("customerId", DataTypes.LongType, true),
                        DataTypes.createStructField("promotionId", DataTypes.LongType, true),
                        DataTypes.createStructField("storeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("storeSales", DataTypes.DoubleType, true),
                        DataTypes.createStructField("storeCost", DataTypes.DoubleType, true),
                        DataTypes.createStructField("unitSales", DataTypes.DoubleType, true)
                ));
StructType schema = DataTypes.createStructType(fields);
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(schema)
                .load(filePath);

Dataset<Sale> salesAsDataSet = salesAsDF.map((MapFunction<Row, Sale>) row -> Sale
                .builder()
                .productId((Integer) row.get(0))
                .timeId((Integer) row.get(1))
                .customerId((Long) row.get(2))
                .promotionId((Integer) row.get(3))
                .storeId((Integer) row.get(4))
                .storeSales((Double) row.get(5))
                .storeCost((Double) row.get(6))
                .unitSales((Double) row.get(7))
                .build(), 
                Encoders.bean(Sale.class));
salesAsDataSet.printSchema();
```
Et les schémas du DataSet est:

```java
// Schema avec la version 1
root
 |-- productId: long (nullable = true)
 |-- timeId: integer (nullable = true)
 |-- customerId: long (nullable = true)
 |-- promotionId: long (nullable = true)
 |-- storeId: integer (nullable = true)
 |-- storeSales: double (nullable = true)
 |-- storeCost: double (nullable = true)
 |-- unitSales: double (nullable = true)

// Schema avec la version 2
root
 |-- customerId: long (nullable = true)
 |-- productId: integer (nullable = true)
 |-- promotionId: integer (nullable = true)
 |-- storeCost: double (nullable = true)
 |-- storeId: integer (nullable = true)
 |-- storeSales: double (nullable = true)
 |-- timeId: integer (nullable = true)
 |-- unitSales: double (nullable = true)


```
</details>

<details><summary>Solution de l'exercice 4 avec DataFrame: Calculer le chiffre d'affaire par magasin

```java
Le résultat peut correspondre à: 
+-------+------------------+
|storeId|        sum(rowCA)|
+-------+------------------+
|     12| 265012.0099999998|
|     22|18206.400000000005|
|      1|164537.21000000037|
|     13| 537768.1800000002|
|      6| 310913.3200000007|
...
```
</summary>  

#### Solution: on peut le faire avec la méthode groupBy et sum ou bien avec la fonction agg !!!

* Solution avec groupBy et sum

```java
ArrayList<StructField> fields = new ArrayList<>(
                Arrays.asList(
                        DataTypes.createStructField("productId", DataTypes.LongType, true),
                        DataTypes.createStructField("timeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("customerId", DataTypes.LongType, true),
                        DataTypes.createStructField("promotionId", DataTypes.LongType, true),
                        DataTypes.createStructField("storeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("storeSales", DataTypes.DoubleType, true),
                        DataTypes.createStructField("storeCost", DataTypes.DoubleType, true),
                        DataTypes.createStructField("unitSales", DataTypes.DoubleType, true)
                ));
StructType schema = DataTypes.createStructType(fields);
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(schema)
                .load(filePath);
Dataset<Row> caByStore = salesAsDF.select(col("storeId"), col("storeSales").multiply(col("unitSales")).as("rowCA"))
                .groupBy(col("storeId"))
                .sum("rowCA").as("ca");
caByStore.show();
```

* Solution avec groupBy et agg (plusieurs fonctions d'aggrégation sont implémentées)
```java
Dataset<Row> caByStore = salesAsDF.select(col("storeId"), col("storeSales").multiply(col("unitSales")).as("rowCA"))
                .groupBy(col("storeId"))
                .agg(sum(col("rowCA")));
```

Le résultat ressemble à:
```java
+-------+------------------+
|storeId|        sum(rowCA)|
+-------+------------------+
|     12| 265012.0099999998|
|     22|18206.400000000005|
|      1|164537.21000000037|
|     13| 537768.1800000002|
|      6| 310913.3200000007|
```

</details>

<details><summary>Solution de l'exercice 5 avec DataFrame: Calculer le nombre d'unités vendues par magasin

```java
Map<Integer, Long> numberUnitsByStore = ...
avec un résultat correspondant à: 
Magasin : 5 a un vendu : 1298 unités
Magasin : 10 a un vendu : 7898 unités
Magasin : 24 a un vendu : 15732 unités
Magasin : 14 a un vendu : 2593 unités
...
```
</summary>  

#### Solution: utiliser la fonction agg présentée dans l'exercice 4 

```java
ArrayList<StructField> fields = new ArrayList<>(
                Arrays.asList(
                        DataTypes.createStructField("productId", DataTypes.LongType, true),
                        DataTypes.createStructField("timeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("customerId", DataTypes.LongType, true),
                        DataTypes.createStructField("promotionId", DataTypes.LongType, true),
                        DataTypes.createStructField("storeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("storeSales", DataTypes.DoubleType, true),
                        DataTypes.createStructField("storeCost", DataTypes.DoubleType, true),
                        DataTypes.createStructField("unitSales", DataTypes.DoubleType, true)
                ));
StructType schema = DataTypes.createStructType(fields);
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(schema)
                .load(filePath);
Dataset<Row> agg = salesAsDF.select(col("storeId"), col("unitSales"))
                .groupBy(col("storeId"))
                .agg(count("unitSales"));
List<Row> rows = agg.collectAsList();
Map<Integer, Long>  storeUnitSales = new HashedMap() ;
rows.stream().forEach(s -> storeUnitSales.put(s.getInt(0),s.getLong(1)));
```

</details>

<details><summary>Solution de l'exercice 6 avec DataFrame: Calculer le chiffre d'affaire par région.  

```java
JavaPairRDD<Integer, Double> caByRegion = ...
avec un résultat correspondant à: 
Region : 23 avec un CA : 537768.1800000002
Region : 89 avec un CA : 151039.54000000007
Region : 26 avec un CA : 265264.4699999993
Region : 47 avec un CA : 310913.3200000007
Region : 2 avec un CA : 76719.89
...
```
</summary>

#### Solution: le moteur sql trouvera lui-même quel schéma de jointure le mieux adapté aux datasets ou le forcer à broadcaster le store.csv dataset !!!

```java
// Lecture du fichier store à broadcaster (fichier très petit)
Dataset<Row> storesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Store.SCHEMA)
                .load(storeFilePath)
                .select(col("id"),col("regionId")) ;
// Charger le fichier sales.csv
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath);
// Sans broadcast
Dataset<Row> caByRegion = salesAsDF.join(storesAsDF, salesAsDF.col("storeId").equalTo(storesAsDF.col("id")))
                .groupBy(col("storeId"))
                .agg(sum(col("storeSales").multiply(col("unitSales"))).as("regionCA"));

// En forçant le broadcast
Dataset<Row> caByRegionWithBroadcast = salesAsDF.join(broadcast(storesAsDF), salesAsDF.col("storeId").equalTo(storesAsDF.col("id")))
                .groupBy(col("storeId"))
                .agg(sum(col("storeSales").multiply(col("unitSales"))).as("regionCA"));
```

</details>

<details><summary>Solution de l'exercice 7 avec DataFrame: Comparer les ventes (en termes de CA) entre les premiers trimestres (Q1) de 1997 et 1998    

```java
JavaPairRDD<Integer, Double>  yearCAQuarter= ...

CA Q1 de l'année 1997 : 460615.02999999735
CA Q1 de l'année 1998 : 965701.8800000021
...
```

</summary>

#### Solution: 

```java
// Lecture du fichier store à broadcaster (fichier très petit)
Dataset<Row> times = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(TimeByDay.SCHEMA)
                .load(timeByDayFilePath)
                .where(col("quarter").equalTo(quarter))
                .select(col("timeId"),col("theYear")) ;


// Charger le fichier sales.csv
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath);

Dataset<Row> caByQuarter = salesAsDF.join(broadcast(times), salesAsDF.col("timeId").equalTo(times.col("timeId")), "right_outer");
caByQuarter.show();

```

</details>

<details><summary>Solution de l'exercice 8 avec DataFrame:       

```
Création du client ES: ESClient es = new ESClient("localhost",9200)  
Envoi de documents vers ES: es.index("sales",map);
...
```

</summary>

#### Solution: 
la fonction util.rowToMap transforme une Row à une Map indexable sur Elasticsearch

```java
// Charger le fichier sales.csv
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath);

salesAsDF.foreachPartition(new ForeachPartitionFunction<Row>() {
            ESClient es = new ESClient("localhost",9200) ;
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                iterator.forEachRemaining(sale -> {
                    es.index("sales",Util.rowToMap(sale));
                });
            }
        });
```

</details>


<details><summary>Solution de l'exercice 9 avec DataFrame: calculer le chiffre d'affaires généré par niveau d'instruction (colonne education du fichier customer.csv.
  Petite contrainte, le fichier customer.csv ne peut pas être broadcasté en l'état.
  Le résultat attendu est:
  
  ```java
  Education level : Graduate Degree a un chiffre d'affaires : 284358.7000000002
  Education level : High School Degree a un chiffre d'affaires : 1614680.6999999923
  Education level : Partial College a un chiffre d'affaires : 506574.38000000064
  Education level : Bachelors Degree a un chiffre d'affaires : 1394302.7699999944
  Education level : Partial High School a un chiffre d'affaires : 1650653.7099999934
  ```
  
  </summary>

#### Solution: 
L'idée de cette question est quand on fait une jointure, on réduit au maximum les données sur lesquelles on ne garde que les donnée nécessaires à la jointure pour réduire le coût du shuffling. 
  
  ```java
  // Lecture du fichier customer à broadcaster
  Dataset<Row> customerEducation = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Customer.SCHEMA)
                .load(customerFilePath)
                .select(col("customerId").as("cid"),col("education"))
                ;

  // Charger le fichier sales.csv
  Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath)
                .select(col("customerId"),col("storeSales").multiply(col("unitSales")).as("rowCA"))
                ;
  Dataset<Row> sum = salesAsDF.join(customerEducation, salesAsDF.col("customerId").equalTo(customerEducation.col("cId")))
                .groupBy(col("education"))
                .agg(sum(col("rowCA")).as("caByEducationLevel"))
                ;
  ```
     
  </details>


<details><summary>Solution de l'exercice 10 avec DataFrame: similaire à l'exercice 8 mais en stockant les résultats sous format CSV sur HDFS.
  </summary>
  
 #### Solution: 
 
  ```java
 // Charger le fichier sales.csv
 Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath) ;
 salesAsDF.write()
          .format("csv")
          .option("sep",";")
          .option("header","false")
          .mode(SaveMode.Ignore)
          .save(destinationFilePath);
  ```
  
  </details>
   </details>
   
<details><summary>Exercie 2: stocker les sales en les partitionnant par date et store et en utilisant ORC, CSV et Parquet comme formats de stockage. Expérimentez également les diffénts modes de sauvegarde (ecraser, mise à jour, ...). Le schéma de la table à stocker est comme suit:

```java
 root
 |-- productId: long (nullable = true)
 |-- timeId: integer (nullable = true)
 |-- customerId: long (nullable = true)
 |-- promotionId: long (nullable = true)
 |-- storeId: integer (nullable = true)
 |-- storeSales: double (nullable = true)
 |-- storeCost: double (nullable = true)
 |-- unitSales: double (nullable = true)
 |-- theDate: string (nullable = true)
 ``` 
  </summary>

#### Solution: 
 
 ```java
 //Charger le fichier times
 Dataset<Row> times = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(TimeByDay.SCHEMA)
                .load(timeByDayFilePath)
                .select(col("timeId").as("tId"),col("theDate"))
                ;

 // Charger le fichier sales.csv
 Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath)
                ;
 Dataset<Row> salesToStore = salesAsDF
                .join(times, salesAsDF.col("timeId").equalTo(times.col("tId")))
                .drop(col("tId"));
 // Stockage en CSV
 salesToStore.write()
                .format("csv")
                .partitionBy("theDate","storeId")
                .option("sep",";")
                .option("header","false")
                .save(destinationDir)
        ;
  // Stockage en ORC
  salesToStore.write()
                .format("orc")
                .partitionBy("theDate","storeId")
                .mode(SaveMode.Overwrite)
                .option("orc.create.index","true")
                .option("orc.bloom.filter.columns","customerId")
                .save(destinationDir);

  // Stockage en Parquet
  salesToStore.write()
                .format("parquet")
                .partitionBy("theDate","storeId")
                .option("compression.codec", "snappy")
                .mode(SaveMode.Ignore)
                .save(destinationDir);

 ```
  
</details>

<details><summary>Exercie 3: refaire l'exercice 9 avec Spark SQL (calculer le chiffre d'affaires généré par niveau d'instruction)
  </summary>

#### Solution: les tables globales sont liés à une base de données globale  global_temp --> il faut utiliser le nom qualifié d'une table, i.e. global_temp.table_name !!! 

```java
// Lecture du fichier customer à broadcaster
Dataset<Row> customerEducation = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Customer.SCHEMA)
                .load(customerFilePath)
                .select(col("customerId").as("cid"),col("education"))
                ;
// Charger le fichier sales.csv
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath)
                .select(col("customerId"),col("storeSales").multiply(col("unitSales")).as("rowCA"))
                ;
// Publication des dataframes sous forme de tables globales accessibles par tous tant que la session est actice
try {
        salesAsDF.createGlobalTempView("sales");
        customerEducation.createGlobalTempView("customer_education");
}
catch(Exception e){
        throw new IllegalArgumentException("");
}

Dataset<Row> results = sparkSession.sql("select education,sum(rowca) from global_temp.sales " +
                "inner join global_temp.customer_education " +
                "on global_temp.sales.customerId=global_temp.customer_education.cId " +
                "group by education");
results.show();
```
</details>

<details><summary>Exercie 4: nettoyage du dataset sales:
  
  ```
  * Supprimer toutes les entrées où storeSales, unitSales ou storeCost n'est pas strictement supérieur à 0
  * Supprimer les lignes où le customerId est vide/null
  * Remplacer le storeId par -1 quand le storeId ne figure pas dans le référentiel store.csv
  * Supprimer la colonne promotionId
  * Ajouter une colonne rowCA=unitSales*storeSales
  ```
  Le schéma du dataset résultat doit être: 
 
 ```
 root
 |-- productId: long (nullable = true)
 |-- timeId: integer (nullable = true)
 |-- customerId: long (nullable = true)
 |-- storeSales: double (nullable = true)
 |-- storeCost: double (nullable = true)
 |-- unitSales: double (nullable = true)
 |-- storeId: integer (nullable = false)
 |-- rowCA: double (nullable = true)
```
  </summary>

#### Solution: 
  
  ```java
  // Lecture du fichier customer à broadcaster
  Dataset<Row> storesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Store.SCHEMA)
                .load(storeFilePath)
                .select(col("id").as("stId"))
                ;
  // Charger le fichier sales.csv
  Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath)
                ;

  Dataset<Row> salesCleant = salesAsDF.drop(col("promotionId"))
                .na()
                .drop("any", new String[]{"storeCost", "storeSales", "unitSales","customerId"})
                .where(col("storeCost").gt(0).and(col("storeSales").gt(0)).and(col("unitSales").gt(0)))
                .join(storesAsDF, salesAsDF.col("storeId").equalTo(storesAsDF.col("stId")),"left_outer")
                .na()
                .fill(ImmutableMap.of("stId", -1))
                .drop(col("storeId"))
                .withColumnRenamed("stId","storeId")
                .withColumn("rowCA",col("storeSales").multiply(col("unitSales")));
                ;
  salesCleant.printSchema();
```
  
</details>

<details><summary>Exercie 5: Quel est le type de jointure effectué dans le code suivant
  
```java
Dataset<Row> storesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Store.SCHEMA)
                .load(storeFilePath)
                .select(col("id").as("stId"))
                ;
 // Charger le fichier sales.csv
 Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath)
                ;

 Dataset<Row> salesCleant = salesAsDF.drop(col("promotionId"))
                .join(storesAsDF, salesAsDF.col("storeId").equalTo(storesAsDF.col("stId")),"left_outer") ;
```
</summary>

#### Pour le savoir, il faut afficher le plan physique avec:

```java
salesCleant.explain() ;
```

#### Ce qui donnera: 

```
== Physical Plan ==
*(2) BroadcastHashJoin [storeId#54], [stId#48], LeftOuter, BuildRight
:- *(2) FileScan csv [productId#50L,timeId#51,customerId#52L,storeId#54,storeSales#55,storeCost#56,unitSales#57] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/D:/BDAcademyCode/bdacademy/data/sales.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<productId:bigint,timeId:int,customerId:bigint,storeId:int,storeSales:double,storeCost:doub...
+- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
   +- *(1) Project [id#0 AS stId#48]
      +- *(1) Filter isnotnull(id#0)
         +- *(1) FileScan csv [id#0] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/D:/BDAcademyCode/bdacademy/data/store.csv], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:int>
```
</details>

<details><summary>Exercie 6: calculer le le CA,le cout et la marge maximal par storeId ainsi qu'au niveau global
 </summary>
  
 #### La solution: 
 
 ```
 // Charger le fichier sales.csv
 Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath)
                .withColumn("rowCA",col("storeSales").multiply(col("unitSales")))
                .withColumn("rowCost",col("storeCost").multiply(col("unitSales")))
                .withColumn("margin",col("storeSales").multiply(col("unitSales")).minus(col("storeCost").multiply(col("unitSales"))))
                ;
Dataset<Row> agg = salesAsDF.rollup("storeId").agg(ImmutableMap.of("rowCA", "sum", "rowCost", "sum","margin","max"));
```

Ce qui donnera comme résultat:

```
+---------+------------------+------------------+------------------+
|  storeId|        sum(rowCA)|      sum(rowCost)|       max(margin)|
+---------+------------------+------------------+------------------+
|        1|164537.21000000037| 65966.98699999962|           65.7225|
|        6| 310913.3200000007|124376.34520000008|             67.66|
|       23|151039.54000000007| 60404.26209999999|             66.81|
|        4| 167369.9499999998|  67236.0063999999|            89.244|
|       16|352874.43999999994| 141130.3777000002|            69.475|
|     null| 5450570.259999958|2182034.5484999768|           90.7776|
|       22|18206.400000000005|         7241.4041|26.174400000000002|
|       14|15890.259999999998| 6332.340100000005|23.107499999999998|
|       21|240343.31000000064| 96383.51200000019|            69.475|
```


</details>

<details><summary>Exercie 7: en utilisant Spark SQL, retourner la table customer en replacant yearlyIncome par la moyenne entre la borne supérieure et la borne inférieure. Le résultat ressemnle à:
 
  ```
 +------------+--------------------+
|yearlyIncome|formatedYearlyIncome|
+------------+--------------------+
| $30K - $50K|             40000.0|
| $70K - $90K|             80000.0|
| $50K - $70K|             60000.0|
| $10K - $30K|             20000.0|
| $30K - $50K|             40000.0|
 ```

 </summary>
  
 #### La solution: 
 
 ```
 // Lecture du fichier customer à broadcaster
 Dataset<Row> customerAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Customer.SCHEMA)
                .load(customerFilePath) ;
 customerAsDF.createGlobalTempView("customer");
 customerAsDF.sparkSession().sqlContext().udf().register("income", new UDF1<String, Double>() {
            @Override
            public Double call(String rawIncome) {
                //"$70K - $90K"
                String[] split = rawIncome.replace('$', ' ').replace('K', ' ').trim().split("-");
                Double lowerBound = Double.valueOf(split[0])*1000 ;
                Double upperBound = Double.valueOf(split[1])*1000 ;
                return (lowerBound+upperBound)/2 ;
            }
        }, DataTypes.DoubleType) ;

 sparkSession.sql("select income(yearlyIncome) from global_temp.customer").show();
```
</details>

<details><summary>Exercie 8: calculer le CA, le cout, la marge et le nombre d'unités vendues par country, state, jour de semaine et par mois. Le résultat ressemble à:

```
|country|    state|   theDay| theMonth|                CA|              COST|            MARGIN|UNITSALES|
+-------+---------+---------+---------+------------------+------------------+------------------+---------+
|   null|  Jalisco|  Tuesday|    April|              34.8|12.706500000000002|           22.0935|     13.0|
| Mexico|  Jalisco|  Tuesday|    April|              34.8|12.706500000000002|           22.0935|     13.0|
```

</summary>

```java
Dataset<Row> salesAsDF = Sale.loadAsDF(sparkSession,salesFilePath) ;
Dataset<Row> customerAsDF = Customer.loadAsDF(sparkSession, customerFilePath).withColumnRenamed("customerId","cId");
Dataset<Row> storeAsDF = Store.loadAsDF(sparkSession, storeFilePath).withColumnRenamed("id","sId").drop(col("country"));
Dataset<Row> timeAsDF = TimeByDay.loadAsDF(sparkSession, timeByDay).withColumnRenamed("timeId","tId");;

Dataset<Row> denormalizedSales = salesAsDF
        .join(broadcast(customerAsDF), salesAsDF.col("customerId").equalTo(customerAsDF.col("cId")), "left_outer")
        .join(broadcast(storeAsDF), salesAsDF.col("storeId").equalTo(storeAsDF.col("sId")), "left_outer")
        .join(broadcast(timeAsDF), salesAsDF.col("timeId").equalTo(timeAsDF.col("tId")), "left_outer") ;

Dataset<Row> agg = denormalizedSales.cube("country","state", "theDay", "theMonth")
                .agg(sum(col("storeSales").multiply(col("unitSales"))).as("CA"),
                     sum(col("storeCost").multiply(col("unitSales"))).as("COST"),
                     sum(col("storeSales").multiply(col("unitSales")).minus(col("storeCost").multiply(col("unitSales")))).as("MARGIN"),
                     sum(col("unitSales")).as("UNITSALES")) ;
agg.show(1000);
```

</details>
