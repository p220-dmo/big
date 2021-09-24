package fr.htc.spark.map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/*
Join datasets
Broadcast files
Compute some KPI
 */
public final class PairMapSalesRDD {

	public static void main(String[] args) throws Exception {

		System.setProperty("hadoop.home.dir", "D:\\dmo\\bin\\hadoup-utils");
		SparkConf conf = new SparkConf();
		conf.setAppName("Spark Count");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// ******************************************************************************************************
		// Chargement d'un fichier texte
		JavaRDD<String> salesAsString = sc.textFile("D:/dmo/dev/bigdata/data/BigData-Dataset/sales.csv");

		JavaRDD<Sales> salesRDD = salesAsString.map(new Function<String, Sales>() {
			private static final long serialVersionUID = 1L;

			public Sales call(String line) throws Exception {
				return Sales.parse(line, ";");
			}

		});

		JavaPairRDD<Integer, Double> storeSalesRDDPair = salesRDD.mapToPair(new PairFunction<Sales, Integer, Double>() {

			private static final long serialVersionUID = 6524467132051382843L;

			public Tuple2<Integer, Double> call(Sales sale) throws Exception {
				Double ca = sale.getUnitSales() * sale.getStoreSales();

				return new Tuple2<Integer, Double>(sale.getStoreId(), ca);
			}
		});
		
		
		//storeSalesRDDPair.collect().forEach(System.out::println);
		
		
		JavaPairRDD<Integer, Double> storeSalesRDDreduce = storeSalesRDDPair.reduceByKey(new Function2<Double, Double, Double>() {
			
			private static final long serialVersionUID = 4945794023312358950L;

			@Override
			public Double call(Double v1, Double v2) throws Exception {
				
				return Double.sum(v1, v2);
			}
		});
		
		
		storeSalesRDDreduce.collect().forEach(System.out::println);

		sc.close();
	}
}