package org.aga.sparkinpractice.exercices;

import avro.shaded.com.google.common.collect.ImmutableMap;
import org.aga.sparkinpractice.model.Customer;
import org.aga.sparkinpractice.model.Sale;
import org.aga.sparkinpractice.model.Store;
import org.aga.sparkinpractice.model.TimeByDay;
import org.aga.sparkinpractice.utils.ESClient;
import org.aga.sparkinpractice.utils.Util;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class SparkDFDS {

    public static void main(String[] args) throws Exception {

        String salesFilePath = "data/sales.csv";
        String storeFilePath = "data/stores.csv";
        String timeByDayFilePath = "data/time_by_day.csv";
        String customerFilePath = "data/customers.csv";
        SparkSession sparkSession = buildSparkSession();
        Dataset<Sale> dataset = Util.loadAsDS(sparkSession, salesFilePath, Sale.class);
        dataset.printSchema();

        Dataset<Row> salesAsDFV1 = readFileAsDataFrameByInferringSchema(sparkSession, salesFilePath);
        salesAsDFV1.printSchema();
        Dataset<Row> salesAsDFV2 = readFileAsDataFrameWithSchema(sparkSession, salesFilePath);
        salesAsDFV2.printSchema();
        Dataset<Sale> saleDatasetWithEncoder = readSalesFileAsDataSetWithEncoder(sparkSession, salesFilePath);
        saleDatasetWithEncoder.printSchema();
        Dataset<Sale> saleDatasetithMap = readSalesFileAsDataSetWithMap(sparkSession, salesFilePath);
        saleDatasetithMap.printSchema();
        computeStoreCAByGroupByAndSum(sparkSession, salesFilePath);
        computeStoreCAByAgg(sparkSession, salesFilePath);
        numberOfUnitsSoltByStore(sparkSession, salesFilePath);
        computeTopPerformerDepartment(sparkSession, salesFilePath, storeFilePath);
        CAOfQuarterBetweenYears(sparkSession, salesFilePath, timeByDayFilePath, "Q1");
        //saveEnrichedSalesToES(sparkSession,salesFilePath) ;
        averageBasketByEducationLevel(sparkSession, salesFilePath, customerFilePath);
        //saveAsCSV(sparkSession,salesFilePath,"data/salesnew.csv") ;
        //savePartitionnedDataAsCSV(sparkSession,salesFilePath,timeByDayFilePath,"data/outpartitionned") ;
        averageBasketByEducationLevelUsingSparkSQL(sparkSession, salesFilePath, customerFilePath);
        cleaningSalesDataSet(sparkSession, salesFilePath, storeFilePath);
        printPhysicalPExecutionPlan(sparkSession, salesFilePath, storeFilePath);
        computeSumCACostByStoreIdAndGlobally(sparkSession, salesFilePath, storeFilePath);
        transformCustomerIncome(sparkSession, customerFilePath);
        testCube(sparkSession, salesFilePath, customerFilePath, storeFilePath, timeByDayFilePath);

    }


    /*
    Solution de l'exercice 1
     */
    public static SparkSession buildSparkSession() {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark dataframe training")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "warehouseLocation") //adding config parameters
                .getOrCreate();

        return sparkSession;
    }

    /*
    Solution de l'exercice 2 V.1
     */
    public static Dataset<Row> readFileAsDataFrameByInferringSchema(SparkSession sparkSession, String filePath) {
        Dataset<Row> salesAsDFWithoutSchemaInferring = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .load(filePath);
        return salesAsDFWithoutSchemaInferring;
    }

    /*
    Solution de l'exercice 2 V.2
     */
    public static Dataset<Row> readFileAsDataFrameWithSchema(SparkSession sparkSession, String filePath) throws Exception {

        Dataset<Row> salesAsDFWithSchemaDefined = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Util.buildSchema(Sale.class))
                .load(filePath);
        return salesAsDFWithSchemaDefined;
    }

    /*
   Solution de l'exercice 3 v.1
    */
    public static Dataset<Sale> readSalesFileAsDataSetWithEncoder(SparkSession sparkSession, String filePath) throws Exception {
        Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Util.buildSchema(Sale.class))
                .load(filePath);
        Encoder<Sale> saleEncoder = Encoders.bean(Sale.class);
        Dataset<Sale> as = salesAsDF.as(saleEncoder);
        as.printSchema();
        return as;
    }

    /*
    Solution de l'exercice 3 v.2
   */
    public static Dataset<Sale> readSalesFileAsDataSetWithMap(SparkSession sparkSession, String filePath) throws Exception {
        Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Util.buildSchema(Sale.class))
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
                .build(), Encoders.bean(Sale.class));
        salesAsDataSet.printSchema();
        return salesAsDataSet;
    }

    /*
    Solution de l'exercice 4 v.1
     */
    public static Dataset<Row> computeStoreCAByGroupByAndSum(SparkSession sparkSession, String filePath) throws Exception {
        Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Util.buildSchema(Sale.class))
                .load(filePath);
        Dataset<Row> caByStore = salesAsDF.select(col("storeId"), col("storeSales").multiply(col("unitSales")).as("rowCA"))
                .groupBy(col("storeId"))
                .sum("rowCA").as("ca");
        caByStore.show();
        return caByStore;
    }

    /*
    Solution de l'exercice 4 v.2
     */
    public static Dataset<Row> computeStoreCAByAgg(SparkSession sparkSession, String filePath) throws Exception {
        Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Util.buildSchema(Sale.class))
                .load(filePath);
        Dataset<Row> caByStore = salesAsDF.select(col("storeId"), col("storeSales").multiply(col("unitSales")).as("rowCA"))
                .groupBy(col("storeId"))
                .agg(sum(col("rowCA")));
        caByStore.show();
        return caByStore;
    }

    /*
    Solution de l'exercice 5
     */
    public static Map<Integer, Long> numberOfUnitsSoltByStore(SparkSession sparkSession, String filePath) throws Exception {
        Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Util.buildSchema(Sale.class))
                .load(filePath);
        Dataset<Row> agg = salesAsDF.select(col("storeId"), col("unitSales"))
                .groupBy(col("storeId"))
                .agg(count("unitSales"));
        List<Row> rows = agg.collectAsList();
        Map<Integer, Long> storeUnitSales = new HashMap();
        rows.stream().forEach(s -> storeUnitSales.put(s.getInt(0), s.getLong(1)));
        storeUnitSales.forEach((k, v) -> System.out.println("Nombre d'unités du magasin " + k + " est de " + v));
        return storeUnitSales;
    }

    /*
    Solution de l'exercice 6
     */
    public static void computeTopPerformerDepartment(SparkSession sparkSession, String salesFilePath, String storeFilePath) throws Exception {
        // Lecture du fichier store à broadcaster (fichier très petit)
        Dataset<Row> storesAsDF = Util.loadAsDF(sparkSession, storeFilePath, Store.class)
                .select(col("id"), col("regionId"));

        storesAsDF.printSchema();

        // Charger le fichier sales.csv

        Dataset<Row> salesAsDF = Util.loadAsDF(sparkSession, salesFilePath, Sale.class);
        // Sans broadcast
        Dataset<Row> caByRegion = salesAsDF.join(storesAsDF, salesAsDF.col("storeId").equalTo(storesAsDF.col("id")))
                .groupBy(col("storeId"))
                .agg(sum(col("storeSales").multiply(col("unitSales"))).as("regionCA"));

        // En forçant le broadcast
        Dataset<Row> caByRegionWithBroadcast = salesAsDF.join(broadcast(storesAsDF), salesAsDF.col("storeId").equalTo(storesAsDF.col("id")))
                .groupBy(col("storeId"))
                .agg(sum(col("storeSales").multiply(col("unitSales"))).as("regionCA"));
        caByRegionWithBroadcast.show();
    }

    /*
    Solution de l'exercice 7
     */
    public static void CAOfQuarterBetweenYears(SparkSession sparkSession, String salesFilePath, String timeByDayFilePath, String quarter) throws Exception {
        // Lecture du fichier store à broadcaster (fichier très petit)
        Dataset<Row> times = Util
                .loadAsDF(sparkSession, timeByDayFilePath, TimeByDay.class)
                .where(col("quarter").equalTo(quarter))
                .select(col("timeId"), col("theYear"));

        // Charger le fichier sales.csv
        Dataset<Row> salesAsDF = Util
                .loadAsDF(sparkSession, salesFilePath, Sale.class);

        Dataset<Row> caByQuarter = salesAsDF.join(broadcast(times), salesAsDF.col("timeId").equalTo(times.col("timeId")), "right_outer");
        caByQuarter.show();
    }

    /*
    Solution de l'exercice 8
     */
    public static void saveEnrichedSalesToES(SparkSession sparkSession, String salesFilePath) throws Exception {
        // Charger le fichier sales.csv
        Dataset<Row> salesAsDF = Util
                .loadAsDF(sparkSession, salesFilePath, Sale.class);

        salesAsDF.foreachPartition(new ForeachPartitionFunction<Row>() {
            ESClient es = new ESClient("localhost", 9200);

            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                iterator.forEachRemaining(sale -> {
                    es.index("sales", Util.rowToMap(sale));
                });
            }
        });
    }

    /*
   Exercice 9
    */
    public static void averageBasketByEducationLevel(SparkSession sparkSession, String salesFilePath, String customerFilePath) throws Exception {
        // Lecture du fichier customer à broadcaster
        Dataset<Row> customerEducation = Util.loadAsDF(sparkSession, customerFilePath, Customer.class)
                .select(col("customerId").as("cid"), col("education"));

        // Charger le fichier sales.csv
        Dataset<Row> salesAsDF = Util.loadAsDF(sparkSession, salesFilePath, Sale.class)
                .select(col("customerId"), col("storeSales").multiply(col("unitSales")).as("rowCA"));
        Dataset<Row> sum = salesAsDF.join(customerEducation, salesAsDF.col("customerId").equalTo(customerEducation.col("cId")))
                .groupBy(col("education"))
                .agg(sum(col("rowCA")).as("caByEducationLevel"));
        sum.show();
    }

    /*
    Solution de l'exercice 10
     */
    public static void saveAsCSV(SparkSession sparkSession, String salesFilePath, String destinationFilePath) throws Exception {
        // Charger le fichier sales.csv
        Dataset<Row> salesAsDF = Util.loadAsDF(sparkSession, salesFilePath, Sale.class);

        salesAsDF.write()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .mode(SaveMode.Ignore)
                .save(destinationFilePath);
    }

    /*
    Exercice 2
     */
    public static void savePartitionnedDataAsCSV(SparkSession sparkSession, String salesFilePath, String timeByDayFilePath, String destinationDir) throws Exception {
        //Charger le fichier times
        Dataset<Row> times = Util.loadAsDF(sparkSession, timeByDayFilePath, TimeByDay.class)
                .select(col("timeId").as("tId"), col("theDate"));

        // Charger le fichier sales.csv
        Dataset<Row> salesAsDF = Util.loadAsDF(sparkSession, salesFilePath, Sale.class);
        Dataset<Row> salesToStore = salesAsDF
                .join(times, salesAsDF.col("timeId").equalTo(times.col("tId")))
                .drop(col("tId"));
        salesToStore.printSchema();

        // Stockage en CVS
        salesToStore.write()
                .format("csv")
                .partitionBy("theDate", "storeId")
                .option("sep", ";")
                .option("header", "false")
                .save(destinationDir)
        ;
        // Stockage en ORC
        salesToStore.write()
                .format("orc")
                .partitionBy("theDate", "storeId")
                .mode(SaveMode.Overwrite)
                .option("orc.create.index", "true")
                .option("orc.bloom.filter.columns", "customerId")
                .save(destinationDir);

        // Stockage en Parquet
        salesToStore.write()
                .format("parquet")
                .partitionBy("theDate", "storeId")
                .option("compression.codec", "snappy")
                .mode(SaveMode.Ignore)
                .save(destinationDir);
    }

    /*
    Exercice 3
     */
    public static void averageBasketByEducationLevelUsingSparkSQL(SparkSession sparkSession, String salesFilePath, String customerFilePath) throws Exception {
        // Lecture du fichier customer à broadcaster
        Dataset<Row> customerEducation = Util.loadAsDF(sparkSession, customerFilePath, Customer.class)
                .select(col("customerId").as("cid"), col("education"));
        // Charger le fichier sales.csv
        Dataset<Row> salesAsDF = Util.loadAsDF(sparkSession, salesFilePath, Sale.class)
                .select(col("customerId"), col("storeSales").multiply(col("unitSales")).as("rowCA"));
        // Publication des dataframes sous forme de tables globales accessibles par tous tant que la session est actice
        try {
            salesAsDF.createGlobalTempView("sales");
            customerEducation.createGlobalTempView("customer_education");
        } catch (Exception e) {
            throw new IllegalArgumentException("");
        }

        Dataset<Row> results = sparkSession.sql("select education,sum(rowca) from global_temp.sales " +
                "inner join global_temp.customer_education " +
                "on global_temp.sales.customerId=global_temp.customer_education.cId " +
                "group by education");
        results.show();

    }

    /*
    Exercie 4
     */
    public static void cleaningSalesDataSet(SparkSession sparkSession, String salesFilePath, String storeFilePath) throws Exception {
        // Lecture du fichier customer à broadcaster
        Dataset<Row> storesAsDF = Util.loadAsDF(sparkSession, storeFilePath, Store.class)
                .select(col("id").as("stId"));
        // Charger le fichier sales.csv
        Dataset<Row> salesAsDF = Util.loadAsDF(sparkSession, salesFilePath, Sale.class);

        Dataset<Row> salesCleant = salesAsDF.drop(col("promotionId"))
                .na()
                .drop("any", new String[]{"storeCost", "storeSales", "unitSales", "customerId"})
                .where(col("storeCost").gt(0).and(col("storeSales").gt(0)).and(col("unitSales").gt(0)))
                .join(storesAsDF, salesAsDF.col("storeId").equalTo(storesAsDF.col("stId")), "left_outer")
                .na()
                .fill(ImmutableMap.of("stId", -1))
                .drop(col("storeId"))
                .withColumnRenamed("stId", "storeId")
                .withColumn("rowCA", col("storeSales").multiply(col("unitSales")));
        salesCleant.printSchema();
        salesCleant.show();
    }

    /*
    Exercie 5
     */
    public static void printPhysicalPExecutionPlan(SparkSession sparkSession, String salesFilePath, String storeFilePath) throws Exception {
        // Lecture du fichier customer à broadcaster
        Dataset<Row> storesAsDF = Util.loadAsDF(sparkSession, storeFilePath, Store.class)
                .select(col("id").as("stId"));
        // Charger le fichier sales.csv
        Dataset<Row> salesAsDF = Util.loadAsDF(sparkSession, salesFilePath, Sale.class);

        Dataset<Row> salesCleant = salesAsDF.drop(col("promotionId"))
                .join(storesAsDF, salesAsDF.col("storeId").equalTo(storesAsDF.col("stId")), "left_outer");
        salesCleant.printSchema();
        salesCleant.explain();
    }

    /*
    Exercice 6
     */
    public static void computeSumCACostByStoreIdAndGlobally(SparkSession sparkSession, String salesFilePath, String storeFilePath) throws Exception {
        // Charger le fichier sales.csv
        Dataset<Row> salesAsDF = Util.loadAsDF(sparkSession, salesFilePath, Sale.class)
                .withColumn("rowCA", col("storeSales").multiply(col("unitSales")))
                .withColumn("rowCost", col("storeCost").multiply(col("unitSales")))
                .withColumn("margin", col("storeSales").multiply(col("unitSales")).minus(col("storeCost").multiply(col("unitSales"))));
        Dataset<Row> agg = salesAsDF.rollup("storeId").agg(ImmutableMap.of("rowCA", "sum", "rowCost", "sum", "margin", "max"));
        agg.show(10000);
    }

    /*
        Exercice 7
     */
    public static void transformCustomerIncome(SparkSession sparkSession, String customerFilePath) throws Exception {
        // Lecture du fichier customer à broadcaster
        Dataset<Row> customerAsDF = Util.loadAsDF(sparkSession, customerFilePath, Customer.class);
        customerAsDF.createGlobalTempView("customer");
        customerAsDF.sparkSession().sqlContext().udf().register("retrieve_income", new UDF1<String, Double>() {
            @Override
            public Double call(String rawIncome) {
                //"$70K - $90K" --> 80000
                String[] split = rawIncome.replace('$', ' ').replace('K', ' ').trim().split("-");
                Double lowerBound = Double.valueOf(split[0]) * 1000;
                Double upperBound = Double.valueOf(split[1]) * 1000;
                return (lowerBound + upperBound) / 2;
            }
        }, DataTypes.DoubleType);

        sparkSession.sql("select yearlyIncome,retrieve_income(yearlyIncome) as formatedYearlyIncome from global_temp.customer").show();
    }

    /*
    Exercice 8
     */
    public static void testCube(SparkSession sparkSession, String salesFilePath, String customerFilePath, String storeFilePath, String timeByDay) throws Exception {
        Dataset<Row> salesAsDF = Util.loadAsDF(sparkSession, salesFilePath, Sale.class);
        Dataset<Row> customerAsDF = Util.loadAsDF(sparkSession, customerFilePath, Customer.class)
                .withColumnRenamed("customerId", "cId");
        Dataset<Row> storeAsDF = Util.loadAsDF(sparkSession, storeFilePath, Store.class)
                .withColumnRenamed("id", "sId").drop(col("country"));
        Dataset<Row> timeAsDF = Util.loadAsDF(sparkSession, timeByDay, TimeByDay.class)
                .withColumnRenamed("timeId", "tId");


        Dataset<Row> denormalizedSales = salesAsDF
                .join(broadcast(customerAsDF), salesAsDF.col("customerId").equalTo(customerAsDF.col("cId")), "left_outer")
                .join(broadcast(storeAsDF), salesAsDF.col("storeId").equalTo(storeAsDF.col("sId")), "left_outer")
                .join(broadcast(timeAsDF), salesAsDF.col("timeId").equalTo(timeAsDF.col("tId")), "left_outer");

        Dataset<Row> agg = denormalizedSales.cube("country", "state", "theDay", "theMonth")
                .agg(sum(col("storeSales").multiply(col("unitSales"))).as("CA"),
                        sum(col("storeCost").multiply(col("unitSales"))).as("COST"),
                        sum(col("storeSales").multiply(col("unitSales")).minus(col("storeCost").multiply(col("unitSales")))).as("MARGIN"),
                        sum(col("unitSales")).as("UNITSALES"))
                .sort("MARGIN");
        agg.show(1000);
    }

}