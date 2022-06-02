package fr.htc.spark.readers;

import static org.apache.spark.sql.types.DataTypes.createStructField;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import fr.htc.spark.beans.TimeByDay;

public class TimeReader implements Reader<Integer, TimeByDay> {

	private final String filePath = "D:/dmo/dev/spark/data/time_by_day.csv";

	@Override
	public JavaRDD<String> getStringRdd() {
		return jsc.textFile(filePath);
	}

	@Override
	public JavaRDD<TimeByDay> getObjectRdd() {
		return getStringRdd().map(csvLine -> TimeByDay.parse(csvLine));
	}

	@Override
	public JavaPairRDD<Integer, TimeByDay> getPairRdd() {
		return null;
	}

	public ArrayList<StructField> getFields() {
		return  new ArrayList<>(
				Arrays.asList(createStructField("TIME_ID", DataTypes.IntegerType, true),
						createStructField("c2", DataTypes.TimestampType, true),
						createStructField("c3", DataTypes.StringType, true),
						createStructField("c4", DataTypes.StringType, true),
						createStructField("YEAR", DataTypes.IntegerType, true),
						createStructField("c6", DataTypes.IntegerType, true),
						createStructField("c7", DataTypes.IntegerType, true),
						createStructField("c8", DataTypes.IntegerType, true),
						createStructField("QUARTER", DataTypes.StringType, true)));
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
				.option("header", "false")
				//.schema(this.getSchema())
				.load(filePath);
	}

	@Override
	public JavaPairRDD<Integer, TimeByDay> getPairRdd(int KeyFlag) {
		// TODO Auto-generated method stub
		return null;
	}

}
