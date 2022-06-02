package fr.htc.spark.readers;

import static fr.htc.spark.common.GlobalConstants.CUSTOMER_ID_FLAG;
import static fr.htc.spark.common.GlobalConstants.PRODUCT_ID_FLAG;
import static fr.htc.spark.common.GlobalConstants.PROMOTION_ID_FLAG;
import static fr.htc.spark.common.GlobalConstants.STORE_ID_FLAG;
import static fr.htc.spark.common.GlobalConstants.TIME_ID_FLAG;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import fr.htc.spark.beans.Sale;
import scala.Tuple2;

public class SaleReader implements Reader<Long, Sale> {
	private final String filePath = "data/sales.csv";

	@Override
	public JavaRDD<String> getStringRdd() {
		return jsc.textFile(filePath);
	}

	@Override
	public JavaRDD<Sale> getObjectRdd() {
		return getStringRdd().map(csvLine -> Sale.parse(csvLine));
	}

	@Override
	public JavaPairRDD<Long, Sale> getPairRdd(int keyFlag) {

		switch (keyFlag) {
		case STORE_ID_FLAG: // 1
			return getObjectRdd().mapToPair(sale -> new Tuple2<Long, Sale>(sale.getStoreId(), sale));
		case TIME_ID_FLAG:// 2
			return getObjectRdd().mapToPair(sale -> new Tuple2<Long, Sale>(sale.getTimeId(), sale));
		case PRODUCT_ID_FLAG:// 3
			return getObjectRdd().mapToPair(sale -> new Tuple2<Long, Sale>(sale.getProductId(), sale));
		case CUSTOMER_ID_FLAG:// 4
			return getObjectRdd().mapToPair(sale -> new Tuple2<Long, Sale>(sale.getCustomerId(), sale));
		case PROMOTION_ID_FLAG:
			return getObjectRdd().mapToPair(sale -> new Tuple2<Long, Sale>(sale.getPromotionId(), sale));
		default:
			System.out.println("incorrect value");
		}
		return null;
	}

	public ArrayList<StructField> getFields() {
		return new ArrayList<>(Arrays.asList(DataTypes.createStructField("productId", DataTypes.LongType, true),
				DataTypes.createStructField("TIME_ID", DataTypes.IntegerType, true),
				DataTypes.createStructField("customerId", DataTypes.LongType, true),
				DataTypes.createStructField("promotionId", DataTypes.LongType, true),
				DataTypes.createStructField("storeId", DataTypes.IntegerType, true),
				DataTypes.createStructField("storeSales", DataTypes.DoubleType, true),
				DataTypes.createStructField("storeCost", DataTypes.DoubleType, true),
				DataTypes.createStructField("unitSales", DataTypes.DoubleType, true)));
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
		return ss.read().format("csv").option("sep", ";").option("header", "false").schema(this.getSchema())
				.load(filePath);
	}

	@Override
	@Deprecated
	/**
	 * @author dmouchene
	 */
	public JavaPairRDD<Long, Sale> getPairRdd() {
		return getPairRdd(0);
	}

}
