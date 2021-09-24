
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class Sale implements Serializable {

    private Integer productId;
    private Integer timeId;
    private Integer customerId;
    private Integer promotionId;
    private Integer storeId;
    private Float storeSales;
    private Float storeCost;
    private Float unitSales;

    private int SIZE = 8;


    public static Sale build(String salesAsString) {

        String[] splits = StringUtils.splitPreserveAllTokens(salesAsString, ";");

        try {
            if (splits.length == 8) {
                Sale s = new Sale();
                s.setProductId(Integer.valueOf(splits[0]));
                s.setTimeId(Integer.valueOf(splits[1]));
                s.setCustomerId(Integer.valueOf(splits[2]));
                s.setPromotionId(Integer.valueOf(splits[3]));
                s.setStoreId(Integer.valueOf(splits[4]));
                s.setStoreSales(Float.valueOf(splits[5]));
                s.setStoreCost(Float.valueOf(splits[6]));
                s.setUnitSales(Float.valueOf(splits[7]));
                return s;
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }


    public Integer getProductId() {
        return productId;
    }

    public void setProductId(Integer productId) {
        this.productId = productId;
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

    public Float getUnitSales() {
        return unitSales;
    }

    public void setUnitSales(Float unitSales) {
        this.unitSales = unitSales;
    }


    public Integer getTimeId() {
        return timeId;
    }

    public void setTimeId(Integer timeId) {
        this.timeId = timeId;
    }

    @Override
    public String toString() {
        return productId +
                ";" + timeId +
                ";" + customerId +
                ";" + promotionId +
                ";" + storeId +
                ";" + storeSales +
                ";" + storeCost +
                ";" + unitSales
                ;
    }

    public Float getGain() {
        return this.getUnitSales() * (this.getStoreSales() - this.getStoreCost());
    }
}
