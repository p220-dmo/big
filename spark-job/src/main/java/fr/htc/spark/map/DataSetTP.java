package fr.htc.spark.map;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetTP {
	
	public static void main(String[] args) throws AnalysisException {
	    // $example on:init_session$
	    SparkSession spark = SparkSession
	      .builder()
	      .appName("Java Spark SQL basic example")
	      .config("spark.sql.warehouse.dir", "file:///D/dmo/dev/tmp/spark-warehouse")
	      .master("local")                           
	      .getOrCreate();


	    String filepath1 = "D:/dmo/dev/bigdata/data/spark-sql/sales1.csv";
	    String filepath2 = "D:/dmo/dev/bigdata/data/spark-sql/sales2.csv";
	    
	    Dataset<Row> df = spark.read()
	    		.format("csv")
	    		.option("header", "true")
	    		.option("inferSchema", "true")
	    		.load(filepath1);
	    
	    df.printSchema();



	    spark.stop();
	  }

}
