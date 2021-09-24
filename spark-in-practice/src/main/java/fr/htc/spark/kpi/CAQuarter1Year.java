package fr.htc.spark.kpi;

import static fr.htc.spark.common.GlobalConstants.QUARTER_Q1;

import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import fr.htc.spark.beans.Sale;
import fr.htc.spark.beans.TimeByDay;
import fr.htc.spark.common.GlobalConstants;
import fr.htc.spark.config.SparkConfig;
import fr.htc.spark.readers.Reader;
import fr.htc.spark.readers.SaleReader;
import fr.htc.spark.readers.TimeReader;
import scala.Tuple2;
import scala.reflect.ClassTag$;

public class CAQuarter1Year {
	

	private static Reader<Integer, TimeByDay> timeReader = new TimeReader();
	private static Reader<Long, Sale> saleReader = new SaleReader();
	
	public static void main(String[] args) {
		
		
			Map<Integer, Integer> quarterTimeId = timeReader.getObjectRdd()                
                .filter((Function<TimeByDay, Boolean>) s -> s.getQuarter().equals(QUARTER_Q1 ))
                .mapToPair((PairFunction<TimeByDay, Integer, Integer>) timeByDay -> new Tuple2<>(timeByDay.getTimeId(), timeByDay.getYear()))
                .collectAsMap();

			Broadcast<Map<Integer, Integer>> filteredTimeIds = SparkConfig.getSparkContext().broadcast(quarterTimeId,   
                                                                                       ClassTag$.MODULE$.apply(Map.class));
			
			saleReader.getObjectRdd()
			.filter((Function<Sale, Boolean>) sale -> filteredTimeIds.value().containsKey(sale.getTimeId()))
			.mapToPair((PairFunction<Sale, Integer, Double>) sale -> 
										new Tuple2<>(filteredTimeIds
														.value()
														.getOrDefault(sale.getTimeId(), 9999), sale.getStoreSales() * sale.getUnitSales()))
			.reduceByKey((ca1, ca2) -> ca1 + ca2)
			.collectAsMap().forEach((k,v) -> System.out.println("CA " + QUARTER_Q1 + " de l'année " + k + " : " + v ));
			

	}

}
