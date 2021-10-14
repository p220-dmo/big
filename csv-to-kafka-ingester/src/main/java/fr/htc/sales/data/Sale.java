package fr.htc.sales.data;

public class Sale {
	
	private Integer productId;
	private Integer timeId;
	private Integer customerId;
	private Integer promotionId;
	private Integer storeId;
	private Float storeSales;
	private Float storeCost;
	private Integer unitSales;
	
	
	
	public static Sale parseLine(String[] csvLineColumns) {
		Sale sale = new Sale();
		
		sale.setProductId(Integer.parseInt(csvLineColumns[0]));
		sale.setTimeId(Integer.parseInt(csvLineColumns[1]));
		sale.setCustomerId(Integer.parseInt(csvLineColumns[2]));
		sale.setPromotionId(Integer.parseInt(csvLineColumns[3]));
		sale.setStoreId(Integer.parseInt(csvLineColumns[4]));
		sale.setStoreSales(Float.parseFloat(csvLineColumns[5]));
		sale.setStoreCost(Float.parseFloat(csvLineColumns[6]));
		sale.setUnitSales(Integer.parseInt(csvLineColumns[0]));
		return sale;
	}


	public Float getCa() {
		return this.storeSales * this.unitSales;
		
	}

	public Integer getProductId() {
		return productId;
	}



	public void setProductId(Integer productId) {
		this.productId = productId;
	}



	public Integer getTimeId() {
		return timeId;
	}



	public void setTimeId(Integer timeId) {
		this.timeId = timeId;
	}



	public Integer getCustomerId() {
		return customerId;
	}



	public void setCustomerId(Integer customerId) {
		this.customerId = customerId;
	}



	public Integer getPromotionId() {
		return promotionId;
	}



	public void setPromotionId(Integer promotionId) {
		this.promotionId = promotionId;
	}



	public Integer getStoreId() {
		return storeId;
	}



	public void setStoreId(Integer storeId) {
		this.storeId = storeId;
	}



	public Float getStoreSales() {
		return storeSales;
	}



	public void setStoreSales(Float storeSales) {
		this.storeSales = storeSales;
	}



	public Float getStoreCost() {
		return storeCost;
	}



	public void setStoreCost(Float storeCost) {
		this.storeCost = storeCost;
	}



	public Integer getUnitSales() {
		return unitSales;
	}



	public void setUnitSales(Integer unitSales) {
		this.unitSales = unitSales;
	}
	
	

}
