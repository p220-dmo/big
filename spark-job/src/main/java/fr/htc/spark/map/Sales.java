package fr.htc.spark.map;

import java.io.Serializable;

public class Sales implements Serializable {

	private static final long serialVersionUID = -2627986873213080230L;

	private Integer productId;
	private Integer timeId;
	private Integer customerId;
	private Integer promotionId;
	private Integer storeId;
	private Double storeSales;
	private Double storeCost;
	private Double unitSales;

	public static Sales parse(String line, String separator) {

		String[] lineArrays = line.split(separator);
		Sales vente = new Sales();
		vente.setProductId(Integer.parseInt(lineArrays[0]));
		vente.setTimeId(Integer.parseInt(lineArrays[1]));
		vente.setCustomerId(Integer.parseInt(lineArrays[2]));
		vente.setProductId(Integer.parseInt(lineArrays[3]));
		vente.setStoreId(Integer.parseInt(lineArrays[4]));
		vente.setStoreSales(Double.parseDouble(lineArrays[5]));
		vente.setStoreCost(Double.parseDouble(lineArrays[6]));
		vente.setUnitSales(Double.parseDouble(lineArrays[7]));

		return vente;

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

	public Double getStoreSales() {
		return storeSales;
	}

	public void setStoreSales(Double storeSales) {
		this.storeSales = storeSales;
	}

	public Double getStoreCost() {
		return storeCost;
	}

	public void setStoreCost(Double storeCost) {
		this.storeCost = storeCost;
	}

	public Double getUnitSales() {
		return unitSales;
	}

	public void setUnitSales(Double unitSales) {
		this.unitSales = unitSales;
	}

	@Override
	public String toString() {
		return "Sales [productId=" + productId + ", timeId=" + timeId + ", customerId=" + customerId + ", promotionId="
				+ promotionId + ", storeId=" + storeId + ", storeSales=" + storeSales + ", storeCost=" + storeCost
				+ ", unitSales=" + unitSales + "]";
	}

}
