package fr.htc.spark.amigo;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import fr.htc.spark.readers.Reader;

public class AmigoReader implements Reader<Integer, Amigo> {
	private final String filePath = "D:/dmo/dev/spark/amig/amigo_201902.csv";

	@Override
	public JavaRDD<String> getStringRdd() {
		return jsc.textFile(filePath);
	}

	@Override
	public JavaRDD<Amigo> getObjectRdd() {
		return getStringRdd().map(csvLine -> Amigo.parse(csvLine));
	}

	@Override
	public JavaPairRDD<Integer, Amigo> getPairRdd() {

		return null;
	}

	public ArrayList<StructField> getFields() {
		return  new ArrayList<>(
				Arrays.asList(
						DataTypes.createStructField("productId", DataTypes.LongType, true),
						DataTypes.createStructField("TIME_ID", DataTypes.IntegerType, true),
						DataTypes.createStructField("customerId", DataTypes.LongType, true),
						DataTypes.createStructField("promotionId", DataTypes.LongType, true),
						DataTypes.createStructField("storeId", DataTypes.IntegerType, true),
						DataTypes.createStructField("storeSales", DataTypes.DoubleType, true),
						DataTypes.createStructField("storeCost", DataTypes.DoubleType, true),
						DataTypes.createStructField("unitSales", DataTypes.DoubleType, true)
						));
	}

	/**
	 * 
	 * @return
	 */
	public StructType getSchema() {
		return DataTypes.createStructType(this.getFields());
	}

	@Override
	public Dataset<Row> getDataSet() {
		return ss.read()
				.format("csv")
				.option("sep", ";")
				.option("header", "true")
				.schema(this.getSchema())
				.load(filePath);
	}

}
