package fr.htc.spark.datafram;

import static fr.htc.spark.common.GlobalConstants.QUARTER_Q1;

import java.util.Map;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import fr.htc.spark.beans.Sale;
import fr.htc.spark.beans.TimeByDay;
import fr.htc.spark.config.SparkConfig;
import fr.htc.spark.readers.Reader;
import fr.htc.spark.readers.SaleReader;
import fr.htc.spark.readers.TimeReader;
import scala.reflect.ClassTag$;

public class CompareQ1ByYear {

	private static Reader<Integer, TimeByDay> timeReader = new TimeReader();
	private static Reader<Long, Sale> saleReader = new SaleReader();


	public static void main(String[] args) {

		Dataset<Row>  times = timeReader.getDataSet();
		
		Dataset<Row> timeYear = times.where(times.col("_c8").equalTo(QUARTER_Q1 ))
		.select(times.col("_c0"),times.col("_c4"));
		
		
		Broadcast<Dataset<Row>> timeYearBrodacted= SparkConfig.getSparkContext().broadcast(timeYear, ClassTag$.MODULE$.apply(Dataset.class));
		timeYearBrodacted.value().show();
		
		Dataset<Row>  salesDataSet = saleReader.getDataSet();
		
		//salesDataSet.show();
		salesDataSet.join(timeYearBrodacted.value(), salesDataSet.col("TIME_ID").equalTo(timeYear.col("_c0")), "right_outer");

	}
}
