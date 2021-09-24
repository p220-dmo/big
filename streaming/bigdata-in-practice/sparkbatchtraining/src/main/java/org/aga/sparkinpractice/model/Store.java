package org.aga.sparkinpractice.model;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@ToString
public class Store {
    private	int id;
    private	String type;
    private	int    regionId;
    private	String name;
    private	int    number;
    private	String streetAddress;
    private	String city;
    private	String state;
    private	int    postalCode;
    private	String country;
    private	String manager;
    private	String phone;
    private	String fax;
    private	String   firstOpenedDate;
    private	String lastRemodelDate;
    private	int    sqft;
    private	int    grocerySqft;
    private	int    frozenSqft;
    private	int    meatSqft;
    private	boolean    coffeeBar;
    private	boolean    videoStore;
    private	boolean    saladBar;
    private	boolean    preparedFood;
    private	boolean    florist;


    public static Store parse(String storeAsString, String sep){
        String[] split = storeAsString.split(sep);
        return Store.builder()
                .id(Integer.valueOf(split[0]))
                .type(String.valueOf(split[1]))
                .regionId(Integer.valueOf(split[2]))
                .name(String.valueOf(split[3]))
                .number(Integer.valueOf(split[4]))
                .streetAddress(String.valueOf(split[5]))
                .city(String.valueOf(split[6]))
                .state(String.valueOf(split[7]))
                .postalCode(Integer.valueOf(split[8]))
                .country(String.valueOf(split[9]))
                .manager(String.valueOf(split[10]))
                .phone(String.valueOf(split[11]))
                .fax(String.valueOf(split[12]))
                .firstOpenedDate(String.valueOf(split[13]))
                .lastRemodelDate(String.valueOf(split[14]))
                .sqft(Integer.valueOf(split[15]))
                .grocerySqft(Integer.valueOf(split[16]))
                .frozenSqft(Integer.valueOf(split[17]))
                .meatSqft(Integer.valueOf(split[18]))
                .coffeeBar(!split[19].equals("0"))
                .videoStore(!split[20].equals("0"))
                .saladBar(!split[21].equals("0"))
                .preparedFood(!split[22].equals("0"))
                .florist(!split[23].equals("0"))
                .build() ;
    }

    public static Store parse(String storeAsString){
        return Store.parse(storeAsString,";") ;
    }


}
