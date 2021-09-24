package fr.htc.spark.kpi;

import java.util.Map;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import fr.htc.spark.beans.Sale;
import fr.htc.spark.beans.Store;
import fr.htc.spark.config.SparkConfig;
import fr.htc.spark.readers.Reader;
import fr.htc.spark.readers.SaleReader;
import fr.htc.spark.readers.StoreReader;

import scala.Tuple2;
import scala.reflect.ClassTag$;

public class CAByRegion {

	private static Reader<Long, Sale> saleReader = new SaleReader();
	private static Reader<Integer, Store> storeReader = new StoreReader();

	public static void main(String[] args) {

		Map<Integer, Integer> storeRegionMapRdd =  storeReader.getObjectRdd()
		.mapToPair(store -> new Tuple2<>(store.getStoreId(), store.getRegionId()))
		.collectAsMap();

		Broadcast<Map<Integer, Integer>> storeRegionMap = SparkConfig.getSparkContext().broadcast(storeRegionMapRdd, ClassTag$.MODULE$.apply(Map.class));

		// Faire un Map-side Join
	saleReader.getObjectRdd()
		                .mapToPair(sale -> new Tuple2<>(storeRegionMap.value().getOrDefault(sale.getStoreId(), -1),
	                            sale.getUnitSales() * sale.getStoreSales()))
		                .reduceByKey((Function2<Double, Double, Double>) (a, b) -> a + b)
		                .collectAsMap()
		                .forEach((k,v) -> System.out.println("Region : " + k + " avec un CA : " + v ));
	}
}
