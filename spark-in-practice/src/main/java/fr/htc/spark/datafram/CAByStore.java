package fr.htc.spark.datafram;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import fr.htc.spark.config.SparkConfig;

public class CAByStore {


	public static void main(String[] args) {
		SparkSession ss = SparkConfig.getSparkSession();

		ArrayList<StructField> fields = new ArrayList<>(
				Arrays.asList(
						DataTypes.createStructField("productId", DataTypes.LongType, true),
						DataTypes.createStructField("timeId", DataTypes.IntegerType, true),
						DataTypes.createStructField("customerId", DataTypes.LongType, true),
						DataTypes.createStructField("promotionId", DataTypes.LongType, true),
						DataTypes.createStructField("storeId", DataTypes.IntegerType, true),
						DataTypes.createStructField("storeSales", DataTypes.DoubleType, true),
						DataTypes.createStructField("storeCost", DataTypes.DoubleType, true),
						DataTypes.createStructField("unitSales", DataTypes.DoubleType, true)
						));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> salesAsDF = ss
				.read()
				.format("csv")
				.option("sep", ";")
				.option("header", "false")
				.schema(schema)
				.load("D:/dmo/dev/spark/data/sales.csv");

		Dataset<Row> caByStore = salesAsDF.select(salesAsDF.col("storeId"), salesAsDF.col("storeSales").multiply(salesAsDF.col("unitSales")).as("rowCA"))
				.groupBy(salesAsDF.col("storeId"))
				.sum("rowCA").as("ca").orderBy(salesAsDF.col("storeId"));
		caByStore.show();
	
        
	}
}
