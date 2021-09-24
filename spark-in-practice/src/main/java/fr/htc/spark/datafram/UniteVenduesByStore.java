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

public class UniteVenduesByStore {


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

		salesAsDF.select(salesAsDF.col("storeId"), salesAsDF.col("unitSales"))
				.groupBy(salesAsDF.col("storeId"))
				.sum("unitSales")
				.orderBy(salesAsDF.col("storeId"))
				.collectAsList();
		
		/**
		List<Row> rows = agg.collectAsList();
		Map<Integer, Double>  storeUnitSales = new HashedMap() ;
		rows.stream().forEach(s -> storeUnitSales.put(s.getInt(0),s.getDouble(1));
		storeUnitSales.keySet().stream().forEach(key -> System.out.println(key + " Value is : " + storeUnitSales.get(key)));
*/

	}
}
