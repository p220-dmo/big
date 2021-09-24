package org.aga.sparkinpractice.exercices;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.aga.sparkinpractice.model.Customer;
import org.aga.sparkinpractice.model.Sale;
import org.aga.sparkinpractice.model.Store;
import org.aga.sparkinpractice.model.TimeByDay;
import org.aga.sparkinpractice.utils.ESClient;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.StreamSupport;

public class SparkCore {

    public static void main(String[] args) {
        String salesFilePath = "data/sales.csv";
        String storeFilePath = "data/stores.csv";
        String timeByDayFilePath = "data/time_by_day.csv";
        String customerFilePath = "data/customers.csv";

        SparkSession sparkSession = buildSparkSession();
        JavaRDD<Sale> saleJavaRDD = readFileAsSaleObjectRDD(sparkSession, salesFilePath);
        computeStoreCAUsingReduceByKey(sparkSession,salesFilePath) ;
        computeStoreCAUsingGroupByKey(sparkSession,salesFilePath) ;
        numberOfUnitsSoltByStore(sparkSession,salesFilePath) ;
        computeTopPerformerDepartment(sparkSession,salesFilePath,storeFilePath) ;
        CAOfQuarterBetweenYears(sparkSession,salesFilePath,timeByDayFilePath,"Q1") ;
        //saveEnrichedSalesToES(sparkSession,salesFilePath) ;
        //saveAsCSV(sparkSession,salesFilePath) ;
        averageBasketByEducationLevel(sparkSession,salesFilePath,customerFilePath) ;
    }

    /*
    Exercice 1
     */
    public static SparkSession buildSparkSession(){
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark core training")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "warehouseLocation") //adding config parameters
                .getOrCreate();
        return sparkSession ;
    }
    /*
    Exercice 2
     */
    public static JavaRDD<String> readFileAsStringObjectRDD(SparkSession sparkSession, String filePath){
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        JavaRDD<String> salesAsStringRDD = jsc.textFile(filePath);
        // Afficher 4 éléments de la RDD
        salesAsStringRDD.take(4).stream().forEach(System.out::println);
        return salesAsStringRDD ;
    }

    /*
    Exercice 3
     */
    public static JavaRDD<Sale> readFileAsSaleObjectRDD(SparkSession sparkSession, String filePath){
        JavaRDD<Sale> salesAsObjects = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(filePath)
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"));
        salesAsObjects.take(5).stream().forEach(System.out::println);
        return salesAsObjects ;
    }

    /*
    Exercice 4
     */
    public static JavaPairRDD<Integer, Double> computeStoreCAUsingReduceByKey(SparkSession sparkSession,String filePath){
        JavaRDD<Sale> salesAsObjects = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(filePath)
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"));

        JavaPairRDD<Integer, Double> storeCA = salesAsObjects
                .mapToPair((PairFunction<Sale, Integer, Double>) sale -> new Tuple2<>(sale.getStoreId(), sale.getStoreSales() * sale.getUnitSales()))
                .reduceByKey((Function2<Double, Double, Double>) (a, b) -> a + b);

        storeCA.collectAsMap().forEach((k,v) -> System.out.println("Magasin : " + k + " a un chiffre d'affaires : " + v));
        return storeCA ;
    }

    /*
    Exercice 4
     */
    public static JavaPairRDD<Integer, Double> computeStoreCAUsingGroupByKey(SparkSession sparkSession,String filePath){

        JavaPairRDD<Integer, Double> storeCA = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(filePath)
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"))
                .mapToPair((PairFunction<Sale, Integer, Double>) sale -> new Tuple2<>(sale.getStoreId(), sale.getStoreSales() * sale.getUnitSales()))
                .groupByKey()
                .mapToPair((PairFunction<Tuple2<Integer, Iterable<Double>>, Integer, Double>) storeSalesCA -> new Tuple2<>(storeSalesCA._1(),
                        StreamSupport.stream(storeSalesCA._2().spliterator(), false).reduce((x, y) -> x + y).get())
                );
        storeCA.collectAsMap().forEach((k,v) -> System.out.println("Magasin : " + k + " a un chiffre d'affaires : " + v));
        return storeCA ;
    }

    /*
    Exercice 5
     */
    public static Map<Integer, Long> numberOfUnitsSoltByStore(SparkSession sparkSession,String filePath){
        Map<Integer, Long> numberUnitsByStore = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(filePath)
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"))
                .mapToPair((PairFunction<Sale, Integer, Double>) sale -> new Tuple2<>(sale.getStoreId(), sale.getUnitSales()))
                .countByKey();
        numberUnitsByStore.forEach((k,v) -> System.out.println("Magasin : " + k + " a un vendu : " + v + " unités"));
        return numberUnitsByStore ;
    }

    /*
    Exercice 6
     */
    public static void computeTopPerformerDepartment(SparkSession sparkSession,String salesFilePath,String storeFilePath){
        // Lecture du fichier store à broadcaster (fichier très petit)
        Map<Integer, Integer> storeRegionMapRdd = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(storeFilePath)
                .mapToPair((PairFunction<String, Integer, Integer>) s -> {
                    Store parse = Store.parse(s);
                    return new Tuple2<>(parse.getId(), parse.getRegionId());
                })
                .collectAsMap();
        Broadcast<Map<Integer, Integer>> storeRegionMap = sparkSession.sparkContext().broadcast(storeRegionMapRdd, ClassTag$.MODULE$.apply(Map.class));

        // Faire un Map-side Join
        JavaPairRDD<Integer, Double> caByRegion = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(salesFilePath)
                .mapToPair((PairFunction<String, Integer, Double>) s -> {
                    Sale sale = Sale.parse(s);
                    return new Tuple2<>(storeRegionMap.value().getOrDefault(sale.getStoreId(), -1),
                            sale.getUnitSales() * sale.getStoreSales());
                })
                .reduceByKey((Function2<Double, Double, Double>) (a, b) -> a + b)
                ;

        caByRegion.collectAsMap().forEach((k,v) -> System.out.println("Region : " + k + " avec un CA : " + v ));
    }

    /*
    Exercice 7
     */
    public static void CAOfQuarterBetweenYears(SparkSession sparkSession,String salesFilePath,String timeByDayFilePath, String quarter){
        // Clé=TimeId et Valeur=Année
        Map<Integer, Integer> quarterTimeId = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(timeByDayFilePath)
                .map((Function<String, TimeByDay>) s -> TimeByDay.parse(s))
                .filter((Function<TimeByDay, Boolean>) s -> s.getQuarter().equals(quarter))
                .mapToPair((PairFunction<TimeByDay, Integer, Integer>) timeByDay -> new Tuple2<>(timeByDay.getTimeId(), timeByDay.getTheYear()))
                .collectAsMap();

        Broadcast<Map<Integer, Integer>> filteredTimeIds = sparkSession.sparkContext().broadcast(quarterTimeId, ClassTag$.MODULE$.apply(Map.class));
        JavaPairRDD<Integer, Double>  yearCAQuarter= JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(salesFilePath)
                .map((Function<String, Sale>) s -> Sale.parse(s))
                .filter((Function<Sale, Boolean>) sale -> filteredTimeIds.value().containsKey(sale.getTimeId()))
                .mapToPair((PairFunction<Sale, Integer, Double>) sale ->
                        new Tuple2<>(filteredTimeIds.value().getOrDefault(sale.getTimeId(), -1), sale.getStoreSales() * sale.getUnitSales()))
                .reduceByKey((x, y) -> x + y);

        yearCAQuarter.collectAsMap().forEach((k,v) -> System.out.println("CA " + quarter + " de l'année " + k + " : " + v ));
    }

    /*
    Exercice 8
     */
    public static void saveEnrichedSalesToES(SparkSession sparkSession,String salesFilePath){
        JavaRDD<Sale> sales = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(salesFilePath)
                .map((Function<String, Sale>) s -> Sale.parse(s));

        sales.foreachPartition((VoidFunction<Iterator<Sale>>) saleIterator -> {
            ESClient es = new ESClient("localhost",9200) ;
            ObjectMapper oMapper = new ObjectMapper();
            saleIterator.forEachRemaining(sale -> {
                Map<String, Object> map = oMapper.convertValue(sale, Map.class);
                es.index("sales",map);
            });
        });

        sales.mapPartitions((FlatMapFunction<Iterator<Sale>, Object>) saleIterator -> {
            ESClient es = new ESClient("localhost",9200) ;
            ObjectMapper oMapper = new ObjectMapper();
            saleIterator.forEachRemaining(sale -> {
                Map<String, Object> map = oMapper.convertValue(sale, Map.class);
                es.index("sales",map);
            });
            return null;
        }).take(1);
    }


    /*
    Exercice 9
     */
    public static void averageBasketByEducationLevel(SparkSession sparkSession,String salesFilePath,String customerFilePath) {
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
    }

    /*
    Exercice 10
     */
    public static void saveAsCSV(SparkSession sparkSession,String salesFilePath){
        JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(salesFilePath)
                .map((Function<String, Sale>) s -> Sale.parse(s))
                .map(s -> s.toCSVFormat(";"))
                .saveAsTextFile("data/salesenriched.csv");
    }

}