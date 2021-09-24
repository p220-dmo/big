package fr.htc.spark.map;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

/*
Join datasets
Broadcast files
Compute some KPI
 */
public final class FlatMapRDD {

	public static void main(String[] args) throws Exception {

		System.setProperty("hadoop.home.dir", "D:\\dmo\\bin\\hadoup-utils");
		SparkConf conf = new SparkConf();
		conf.setAppName("Spark Count");
		conf.setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(conf);

		// ******************************************************************************************************
		// Chargement d'un fichier texte
		JavaRDD<String> productAsString = sc.textFile("D:/dmo/dev/bigdata/data/BigData-Dataset/product.csv");

		JavaRDD<String> productNamesListOfWords = productAsString.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String line) throws Exception {
				String fields[] = line.split(";");
				String productName = fields[1];
				String[] words = productName.split(" ");

				return Arrays.stream(words).iterator();
			}
		});

		long count = productAsString.count();
		System.out.println("Line count : " + count);
		count = productNamesListOfWords.count();
		System.out.println("Word count : " + count);

		sc.close();
	}
}