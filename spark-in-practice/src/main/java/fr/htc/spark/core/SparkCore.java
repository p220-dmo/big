package fr.htc.spark.core;

import fr.htc.spark.beans.Sale;
import fr.htc.spark.readers.Reader;
import fr.htc.spark.readers.SaleReader;
import scala.Tuple2;

public class SparkCore {
	
	private static Reader<Long, Sale> saleReader = new SaleReader();

	public static void main(String[] args) {
		
		saleReader.getObjectRdd()
		.mapToPair(sale -> new Tuple2<Long, Double>(sale.getStoreId(), sale.getStoreSales() * sale.getUnitSales()))
		.reduceByKey((ca1 , ca2) -> ca1 + ca2)
		.foreach(l -> System.out.println("Magasin : " + l._1 + " a un CA de : + "  + l._2));

	}
}
