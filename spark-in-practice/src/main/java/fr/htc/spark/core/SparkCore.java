package fr.htc.spark.core;

import java.util.List;

import fr.htc.spark.beans.Sale;
import fr.htc.spark.readers.Reader;
import fr.htc.spark.readers.SaleReader;
import scala.Tuple2;

public class SparkCore {
	
	private static Reader<Long, Sale> saleReader = new SaleReader();

	public static void main(String[] args) {
		
		List<Tuple2<Long, Sale>> listTuple = 
				saleReader.getObjectRdd()
				.mapToPair(sale -> new Tuple2<Long, Sale>(sale.getStoreId(), sale))
				.collect();
		
		for (Tuple2<Long, Sale> tuple2 : listTuple) {
			//do some thing
			if(((Sale)(tuple2._2)).getProductId() == Long.parseLong("1355")) {
				System.out.println("");
			}
			System.out.println(tuple2._1 + " : " + tuple2._2);
		}
		

	}
}
