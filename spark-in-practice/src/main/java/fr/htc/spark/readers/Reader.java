package fr.htc.spark.readers;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import fr.htc.spark.config.SparkConfig;

public interface Reader<ID,TYPE> {
	
	JavaSparkContext jsc = SparkConfig.getJavaSparkContext();
	SparkSession ss = SparkConfig.getSparkSession();
	
	public JavaRDD<String> getStringRdd();
	
	public JavaRDD<TYPE> getObjectRdd();
	
	public JavaPairRDD<ID, TYPE> getPairRdd();

	Dataset<Row> getDataSet();

}
