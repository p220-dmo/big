package fr.htc.spark.config;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConfig {

	/**
	 * 
	 * @return
	 */
	public static JavaSparkContext getJavaSparkContext() {
		return JavaSparkContext.fromSparkContext(getSparkSession().sparkContext());
	}

	public static SparkSession getSparkSession() {
		return SparkSession.builder()
				.appName("Mining Frequent Itemset/Assiocation rules from purchasing basket")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "warehouseLocation") // adding config parameters
				.getOrCreate();
	}
	
	public static SparkContext getSparkContext() {
		return getSparkSession().sparkContext();
	}
	
	
	

}
