# Spark Core

## Introduction
La description des données utilisées pour ses exercices est accessible [ici](https://github.com/Ahmed-Gater/spark-in-practice/blob/master/datasetdescription.md).
La version de Spark pour ces exercices est 2.4.2 et scala 11.  

## Exercices
<details><summary>Exercice 1: Quel est le point d'entrée pour un Job Spark ?</summary>
<p>

#### C'est SparkSession !!!
```
import org.apache.spark.sql.SparkSession;
SparkSession sparkSession = SparkSession
                .builder()
                .appName("Mining Frequent Itemset/Assiocation rules from purchasing basket")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "warehouseLocation") //adding config parameters
                .getOrCreate();
```
</p>
</details>

<details><summary>Exercice 2: Charger le fichier des ventes (sales.csv) dans une RDD de type String

```
JavaRDD<String> salesAsStringRDD = ...
```

</summary>  
<p>
  
#### Charger le fichier texte en utilisant la méthode textFile de Java Spark Contexte !!!
```
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
JavaRDD<String> salesAsStringRDD = jsc.textFile("data/sales.csv");
// Afficher 4 éléments de la RDD
salesAsStringRDD.take(4).stream().forEach(System.out::println);
```
avec comme résultat:

```
product_id;time_id;customer_id;promotion_id;store_id;store_sales;store_cost;unit_sales
337;371;6280;0;2;1.5;0.51;2.0
1512;371;6280;0;2;1.62;0.6318;3.0
963;371;4018;0;2;2.4;0.72;1.0

```
</p>
</details>

<details><summary>Exercice 3: Charger le fichier des ventes (sales.csv) dans une RDD de type Sale. La classe Sale est aussi à développer

```
JavaRDD<Sale> salesAsSaleObject = ...
```
</summary>  

#### Charger le fichier comme à l'exercice 2 et mapper chaque ligne vers un objet Sale qu'on aura créé !!!

```
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

JavaRDD<Sale> salesAsObjects = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile("data/sales.csv")
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"));
```
avec comme résultat:

```
Sale(productId=337, timeId=371, customerId=6280, promotionId=0, storeId=2, storeSales=1.5, storeCost=0.51, unitSales=2.0)
Sale(productId=1512, timeId=371, customerId=6280, promotionId=0, storeId=2, storeSales=1.62, storeCost=0.6318, unitSales=3.0)
Sale(productId=963, timeId=371, customerId=4018, promotionId=0, storeId=2, storeSales=2.4, storeCost=0.72, unitSales=1.0)
```
</details>

<details><summary>Exercice 4: Calculer le chiffre d'affaire par magasin

```
JavaPairRDD<Integer, Double> storeCA = ...
avec un résultat correspondant à: 
Magasin : 23 a un chiffre d'affaires : 151039.54000000007
Magasin : 17 a un chiffre d'affaires : 502334.1299999994
Magasin : 8 a un chiffre d'affaires : 265264.4699999993
Magasin : 11 a un chiffre d'affaires : 364652.1300000001
Magasin : 20 a un chiffre d'affaires : 68089.59
...
```
</summary>  

#### On peut le faire avec reduceByKey ou groupByKey. Cependant, il faut privilègier reduceByKey, car un calcul intermédiaire se fait au niveau de chaque partition avant d'envoyer les résultats sur le réseau. Ce qui fait qu'on n'envoie qu'un tuple par clé et par partition. Conséquence, moins de trafic réseau !!!

* Solution avec reduceByKey

```
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

JavaPairRDD<Integer, Double> storeCA = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(filePath)
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"));
                .mapToPair((PairFunction<Sale, Integer, Double>) sale -> new Tuple2<>(sale.getStoreId(), sale.getStoreSales() * sale.getUnitSales()))
                .reduceByKey((Function2<Double, Double, Double>) (a, b) -> a + b);
storeCA.collectAsMap().forEach((k,v) -> System.out.println("Magasin : " + k + " a un chiffre d'affaires : " + v));
```

* Solution avec groupByKey

```
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

JavaPairRDD<Integer, Double> storeCA = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(filePath)
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"))
                .mapToPair((PairFunction<Sale, Integer, Double>) sale -> new Tuple2<>(sale.getStoreId(), sale.getStoreSales() * sale.getUnitSales()))
                .groupByKey()
                .mapToPair((PairFunction<Tuple2<Integer, Iterable<Double>>, Integer, Double>) storeSalesCA -> new Tuple2<>(storeSalesCA._1(),
                        StreamSupport.stream(storeSalesCA._2().spliterator(), false).reduce((x, y) -> x + y).get())
                );
        storeCA.collectAsMap().forEach((k,v) -> System.out.println("Magasin : " + k + " a un chiffre d'affaires : " + v));
  
```

</details>

<details><summary>Exercice 5: Calculer le nombre d'unités vendues par magasin

```
Map<Integer, Long> numberUnitsByStore = ...
avec un résultat correspondant à: 
Magasin : 5 a un vendu : 1298 unités
Magasin : 10 a un vendu : 7898 unités
Magasin : 24 a un vendu : 15732 unités
Magasin : 14 a un vendu : 2593 unités
...
```
</summary>  

#### Mapper la RDD vers une Map de type Pair et faire un countByKey 
```
Map<Integer, Long> numberUnitsByStore = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(filePath)
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"))
                .mapToPair((PairFunction<Sale, Integer, Double>) sale -> new Tuple2<>(sale.getStoreId(), sale.getUnitSales()))
                .countByKey();
numberUnitsByStore.forEach((k,v) -> System.out.println("Magasin : " + k + " a un vendu : " + v + " unités"));
```

</details>

<details><summary>Exercice 6: Calculer le chiffre d'affaire par région.  

```
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

#### Broadcaster le dataset store (qui est très léger) et faire un Map-side Join !!!

```
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.reflect.ClassTag$;

// Lecture du fichier store à broadcaster (fichier très petit)
Map<Integer, Integer> storeRegionMapRdd = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile("data/store.csv")
                .mapToPair((PairFunction<String, Integer, Integer>) s -> {
                    Store parse = Store.parse(s);
                    return new Tuple2<>(parse.getId(), parse.getRegionId());
                })
                .collectAsMap();
Broadcast<Map<Integer, Integer>> storeRegionMap = sparkSession.sparkContext().broadcast(storeRegionMapRdd, ClassTag$.MODULE$.apply(Map.class));

// Faire un Map-side Join
JavaPairRDD<Integer, Double> caByRegion = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile("data/sales.csv")
                .mapToPair((PairFunction<String, Integer, Double>) s -> {
                    Sale sale = Sale.parse(s);
                    return new Tuple2<>(storeRegionMap.value().getOrDefault(sale.getStoreId(), -1),
                            sale.getUnitSales() * sale.getStoreSales());
                })
                .reduceByKey((Function2<Double, Double, Double>) (a, b) -> a + b);

caByRegion.collectAsMap().forEach((k,v) -> System.out.println("Region : " + k + " avec un CA : " + v ));
```

</details>

<details><summary>Exercice 7: Comparer les ventes (en termes de CA) entre les premiers trimestres (Q1) de 1997 et 1998    

```
JavaPairRDD<Integer, Double>  yearCAQuarter= ...

CA Q1 de l'année 1997 : 460615.02999999735
CA Q1 de l'année 1998 : 965701.8800000021
...
```

</summary>

```
// Clé=TimeId et Valeur=Année
Map<Integer, Integer> quarterTimeId = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(timeByDayFilePath)
                .map((Function<String, TimeByDay>) s -> TimeByDay.parse(s))
                .filter((Function<TimeByDay, Boolean>) s -> s.getQuarter().equals(quarter))
                .mapToPair((PairFunction<TimeByDay, Integer, Integer>) timeByDay -> new Tuple2<>(timeByDay.getTimeId(),         
                                    timeByDay.getTheYear()))
                .collectAsMap();

Broadcast<Map<Integer, Integer>> filteredTimeIds = sparkSession.sparkContext().broadcast(quarterTimeId,   
                                                                                       ClassTag$.MODULE$.apply(Map.class));
JavaPairRDD<Integer, Double>  yearCAQuarter= JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(salesFilePath)
                .map((Function<String, Sale>) s -> Sale.parse(s))
                .filter((Function<Sale, Boolean>) sale -> filteredTimeIds.value().containsKey(sale.getTimeId()))
                .mapToPair((PairFunction<Sale, Integer, Double>) sale ->
                        new Tuple2<>(filteredTimeIds.value().getOrDefault(sale.getTimeId(), -1), sale.getStoreSales() * 
                                                                                                  sale.getUnitSales()))
                .reduceByKey((x, y) -> x + y);
yearCAQuarter.collectAsMap().forEach((k,v) -> System.out.println("CA " + quarter + " de l'année " + k + " : " + v ));
```

</details>

<details><summary>Exercice 8: Aprés avoir joint les sales, time_id, store_id, stocker le résultat sur Elasticsearch. Un objet Java permettant de communiquer avec Elasticsearch est fourni dans le git. Il offre une méthode prenant comme entrée une Map<> et qui l'envoie vers Elasticsearch. Une description de l'installation d'un noeud Elasticsearch via docker est également fournie dans le git. Notez que le but dans cet exercice n'est pas de comprendre le fonctionnement d'ES, mais de savoir comment communiquer avec un service externe à partir de Spark.      

```
Création du client ES: ESClient es = new ESClient("localhost",9200)  
Envoi de documents vers ES: es.index("sales",map);
...
```

</summary>

```
import com.fasterxml.jackson.databind.ObjectMapper;
import org.aga.sparkinpractice.clients.ESClient;
import org.aga.sparkinpractice.model.Sale;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;

JavaRDD<Sale> sales = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(salesFilePath)
                .map((Function<String, Sale>) s -> Sale.parse(s));
// Option 1 avec foreachPartition
sales.foreachPartition((VoidFunction<Iterator<Sale>>) saleIterator -> {
            ESClient es = new ESClient("localhost",9200) ;
            ObjectMapper oMapper = new ObjectMapper();
            saleIterator.forEachRemaining(sale -> {
                Map<String, Object> map = oMapper.convertValue(sale, Map.class);
                es.index("sales",map);
            });
});
// Option 2 avec mapPartitions
sales.mapPartitions((FlatMapFunction<Iterator<Sale>, Object>) saleIterator -> {
            ESClient es = new ESClient("localhost",9200) ;
            ObjectMapper oMapper = new ObjectMapper();
            saleIterator.forEachRemaining(sale -> {
                Map<String, Object> map = oMapper.convertValue(sale, Map.class);
                es.index("sales",map);
            });
            return null;
}).take(1);

```
</details>

<details><summary>Exercice 9: calculer le chiffre d'affaires généré par niveau d'instruction (colonne education du fichier customer.csv.
  Petite contrainte, le fichier customer.csv ne peut pas être broadcasté en l'état.
  Le résultat attendu est:
  
  ```
  Education level : Graduate Degree a un chiffre d'affaires : 284358.7000000002
  Education level : High School Degree a un chiffre d'affaires : 1614680.6999999923
  Education level : Partial College a un chiffre d'affaires : 506574.38000000064
  Education level : Bachelors Degree a un chiffre d'affaires : 1394302.7699999944
  Education level : Partial High School a un chiffre d'affaires : 1650653.7099999934
  ```
  
  </summary>
  L'idée de cette question est quand on fait une jointure, on réduit au maximum les données sur lesquelles on ne garde que les donnée nécessaires à la jointure pour réduire le coût du shuffling. 
  
  ```
  JavaPairRDD<Long, String> customerEducationLevel = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(customerFilePath)
                .mapToPair((PairFunction<String, Long, String>) s -> {
                    Customer parse = Customer.parse(s);
                    return new Tuple2<>(parse.getCustomerId(), parse.getEducation());
                });

        JavaPairRDD<Long, Double> customerSales = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(salesFilePath)
                .mapToPair((PairFunction<String, Long, Double>) s -> {
                            Sale parse = Sale.parse(s);
                            return new Tuple2<>(parse.getCustomerId(), parse.getUnitSales() * parse.getStoreSales());
                        }
                );
        JavaPairRDD<String, Double> educationExpenses =
                customerSales.join(customerEducationLevel)
                        .values()
                        .mapToPair((PairFunction<Tuple2<Double, String>, String, Double>) t ->
                                    new Tuple2<>(t._2(), t._1()))
                        .reduceByKey((Function2<Double, Double, Double>) (o, o2) -> o + o2);
        educationExpenses.collectAsMap().forEach((k,v) -> System.out.println("Education level : " + k + " a un chiffre d'affaires : " + v));
  ```
     
  </details>


<details><summary>Exercice 10: similaire à l'exercice 8 mais en stockant les résultats sous format CSV sur HDFS.
  </summary>
  
  L'idée est de transformer le résultat de l'enrichissement à une JavaRDD<String> où chaque entrée représente l'objet enrichi sours format CSV. 
  
  ```
  JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(salesFilePath)
                .map((Function<String, Sale>) s -> Sale.parse(s))
                .map(s -> s.toCSVFormat(";"))
                .saveAsTextFile("data/salesenriched.csv");
  ```
  
  </details>



