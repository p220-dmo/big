package fr.htc.spark.config;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConfig {

	/**
	 * @return JavaSparkContext
	 */
	public static JavaSparkContext getJavaSparkContext() {
		return JavaSparkContext.fromSparkContext(getSparkSession().sparkContext());
	}

	/**
	 * @return SparkSession
	 */
	public static SparkSession getSparkSession() {
		return SparkSession.builder()
				.appName("MySpark App")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "warehouseLocation")
				.getOrCreate();
	}

	/**
	 * @return SparkContext
	 */
	public static SparkContext getSparkContext() {
		return getSparkSession().sparkContext();
	}

}
