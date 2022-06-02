package fr.htc.spark.readers;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import fr.htc.spark.beans.Store;
import scala.Tuple2;

public class StoreReader implements Reader<Integer, Store> {
	private final String filePath = "D:/dmo/dev/spark/data/stores.csv";

	@Override
	public JavaRDD<String> getStringRdd() {
		return jsc.textFile(filePath);
	}

	@Override
	public JavaRDD<Store> getObjectRdd() {
		return getStringRdd().map(csvLine -> Store.parse(csvLine));
	}

	@Override
	public JavaPairRDD<Integer, Store> getPairRdd() {

		return getObjectRdd().mapToPair(store -> new Tuple2<>(store.getStoreId(), store));
	}



	@Override
	public Dataset<Row> getDataSet() {

		return null;
	}

	@Override
	public JavaPairRDD<Integer, Store> getPairRdd(int KeyFlag) {
		// TODO Auto-generated method stub
		return null;
	}





}
