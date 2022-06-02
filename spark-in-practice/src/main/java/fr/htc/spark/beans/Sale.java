package fr.htc.spark.beans;

import java.io.Serializable;

public class Sale implements Serializable {
	
	private static final long serialVersionUID = -415348036409985383L;

	private long productId;
	private long timeId;
	private long customerId;
	private long promotionId;
	private long storeId;
	private double storeSales;
	private double storeCost;
	private double unitSales;

	public long getProductId() {
		return productId;
	}

	public void setProductId(long productId) {
		this.productId = productId;
	}

	public long getTimeId() {
		return timeId;
	}

	public void setTimeId(long timeId) {
		this.timeId = timeId;
	}

	public long getCustomerId() {
		return customerId;
	}

	public void setCustomerId(long customerId) {
		this.customerId = customerId;
	}

	public long getPromotionId() {
		return promotionId;
	}

	public void setPromotionId(long promotionId) {
		this.promotionId = promotionId;
	}

	public long getStoreId() {
		return storeId;
	}

	public void setStoreId(long storeId) {
		this.storeId = storeId;
	}

	public double getStoreSales() {
		return storeSales;
	}

	public void setStoreSales(double storeSales) {
		this.storeSales = storeSales;
	}

	public double getStoreCost() {
		return storeCost;
	}

	public void setStoreCost(double storeCost) {
		this.storeCost = storeCost;
	}

	public double getUnitSales() {
		return unitSales;
	}

	public void setUnitSales(double unitSales) {
		this.unitSales = unitSales;
	}

	/**
	 * 
	 * @param line
	 * @return
	 */
	public static Sale parse(String line) {

		String[] columns = line.split(";");
		Sale sale = new Sale();

		sale.setProductId(Long.parseLong(columns[0]));
		sale.setTimeId(Long.parseLong(columns[1]));
		sale.setCustomerId(Long.parseLong(columns[2]));
		sale.setPromotionId(Long.parseLong(columns[3]));
		sale.setStoreId(Long.parseLong(columns[4]));
		sale.setStoreSales(Double.parseDouble(columns[5]));
		sale.setStoreCost(Double.parseDouble(columns[6]));
		sale.setUnitSales(Double.parseDouble(columns[7]));

		return sale;
	}

	@Override
	public String toString() {
		return "Sale [productId=" + productId + ", timeId=" + timeId + ", customerId=" + customerId + ", promotionId="
				+ promotionId + ", storeId=" + storeId + ", storeSales=" + storeSales + ", storeCost=" + storeCost
				+ ", unitSales=" + unitSales + "]";
	}

	public Object toCSVFormat(String string) {
		// TODO Auto-generated method stub
		return null;
	}

	
	
}
