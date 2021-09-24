package fr.htc.spark.map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/*
Join datasets
Broadcast files
Compute some KPI
 */
public final class MapRDD {

	public static void main(String[] args) throws Exception {

		System.setProperty("hadoop.home.dir", "D:\\dmo\\bin\\hadoup-utils");		
		SparkConf conf = new SparkConf();
		conf.setAppName("Spark Count");
		conf.setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(conf);

		// Chargement d'un fichier texte sales
		JavaRDD<String> salesAsString = sc.textFile("D:/dmo/dev/bigdata/data/BigData-Dataset/sales.csv");

		JavaRDD<Double> benifits = salesAsString.map(new Function<String, Double>() {

			private static final long serialVersionUID = -217152653860993819L;

			public Double call(String line) throws Exception {
				String fields[] = line.split(";");

				double a = Double.valueOf(fields[5]);
				double b = Double.valueOf(fields[6]);
				double c = Double.valueOf(fields[7]);
				System.out.println(a - (b * c));
				return a - (b * c);
			}
		});


		long count = salesAsString.count();
		System.out.println("Line count : " + count);
		count = benifits.count();
		System.out.println("benifits count : " + count);

		sc.close();
	}
}