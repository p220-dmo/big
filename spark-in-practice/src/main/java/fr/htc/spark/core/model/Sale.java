package fr.htc.spark.core.model;

import java.io.Serializable;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@ToString
public class Sale implements Serializable {
	private static final long serialVersionUID = -8144507481550068734L;
	private int productId ;
    private int timeId ;
    private  long customerId;
    private int promotionId;
    private int storeId;
    private double storeSales;
    private double storeCost;
    private double unitSales ;

    public String toCSVFormat(String sep){
        return this.productId + sep
                + productId + sep
                + timeId + sep
                + customerId + sep
                + promotionId + sep
                + storeId + sep
                + storeSales + sep
                + unitSales ;
    }

    public static Sale parse(String saleAsString, String sep){
        String[] split = saleAsString.split(sep);
        return Sale.builder()
                .productId(Integer.valueOf(split[0]))
                .timeId(Integer.valueOf(split[1]))
                .customerId(Long.valueOf(split[2]))
                .promotionId(Integer.valueOf(split[3]))
                .storeId(Integer.valueOf(split[4]))
                .storeSales(Double.valueOf(split[5]))
                .storeCost(Double.valueOf(split[6]))
                .unitSales(Double.valueOf(split[7]))
                .build() ;
    }

    public static Sale parse(String salesAsString){
        return parse(salesAsString,";") ;
    }


}
