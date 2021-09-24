package fr.htc.spark.map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/*
Join datasets
Broadcast files
Compute some KPI
 */
public final class PairMapRDD {

	public static void main(String[] args) throws Exception {

		System.setProperty("hadoop.home.dir", "D:\\dmo\\bin\\hadoup-utils");
		SparkConf conf = new SparkConf();
		conf.setAppName("Spark Count");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// ******************************************************************************************************
		// Chargement d'un fichier texte
		JavaRDD<String> salesAsString = sc.textFile("D:/dmo/dev/bigdata/data/BigData-Dataset/sales.csv");

		JavaPairRDD<Long, Double> productIdAndPricePair = salesAsString.mapToPair(new PairFunction<String, Long, Double>() {
			private static final long serialVersionUID = 1L;

					public Tuple2<Long, Double> call(String line) throws Exception {

						String[] fields = line.split(";");
						Long productId = Long.valueOf(fields[0]);
						Double unitSales = Double.valueOf(fields[7]);

						Tuple2<Long, Double> res = new Tuple2<Long, Double>(productId, unitSales);

						return res;
					}
				});

		long count = salesAsString.count();
		System.out.println("Line count : " + count);
		count = productIdAndPricePair.count();
		System.out.println("Pair count : " + count);
		
		for (Tuple2<Long, Double> tuple : productIdAndPricePair.collect()) {
			System.out.println(tuple.toString());
		}
		
		JavaPairRDD<Long, Double> reduceMapResult = productIdAndPricePair.reduceByKey(new Function2<Double, Double, Double>() {
			
			public Double call(Double v1, Double v2) throws Exception {
				
				return v1 + v2;
			}
		});
		
		
		
		
		for (Tuple2<Long, Double> ketValuePair : reduceMapResult.collect()) {
			System.out.println(ketValuePair.toString());
		}
		sc.close();
	}
}