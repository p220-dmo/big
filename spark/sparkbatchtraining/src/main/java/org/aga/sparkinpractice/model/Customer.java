package org.aga.sparkinpractice.model;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@ToString
public class Customer {
    long customerId	;
    long accountNum	;
    String lName	;
    String fName	;
    String mi	;
    String address1	;
    String address2	;
    String address3	;
    String address4	;
    String city	;
    String stateProvince	;
    int postalCode	;
    String country	;
    int customerRegionId	;
    String phone1	;
    String phone2	;
    String birthdate	;
    String maritalStatus	;
    String yearlyIncome	;
    String gender	;
    int totalChildren	;
    int numChildrenAtHome	;
    String education	;
    String dateAccountPpened	;
    String memberCard	;
    String occupation	;
    boolean houseOwner	;
    int numCarsOwned	;
    String fullName	;

    public static Customer parse(String customerAsString, String sep){
        String[] split = customerAsString.split(sep);
        return Customer.builder()
            .customerId(Integer.valueOf(split[0]))
                .accountNum(Long.valueOf(split[1]))
                .lName(String.valueOf(split[2]))
                .fName(String.valueOf(split[3]))
                .mi(String.valueOf(split[4]))
                .address1(String.valueOf(split[5]))
                .address2(String.valueOf(split[6]))
                .address3(String.valueOf(split[7]))
                .address4(String.valueOf(split[8]))
                .city(String.valueOf(split[9]))
                .stateProvince(String.valueOf(split[10]))
                .postalCode(Integer.valueOf(split[11]))
                .country(String.valueOf(split[12]))
                .customerRegionId(Integer.valueOf(split[13]))
                .phone1(String.valueOf(split[14]))
                .phone2(String.valueOf(split[15]))
                .birthdate(String.valueOf(split[16]))
                .maritalStatus(String.valueOf(split[17]))
                .yearlyIncome(String.valueOf(split[18]))
                .gender(String.valueOf(split[19]))
                .totalChildren(Integer.valueOf(split[20]))
                .numChildrenAtHome(Integer.valueOf(split[21]))
                .education(String.valueOf(split[22]))
                .dateAccountPpened(String.valueOf(split[23]))
                .memberCard(String.valueOf(split[24]))
                .occupation(String.valueOf(split[25]))
                .houseOwner(!split[26].equals("N"))
                .numCarsOwned(Integer.valueOf(split[27]))
                .fullName(String.valueOf(split[28]))
                .build() ;
    }

    public static Customer parse(String s){
        return parse(s,";") ;
    }
}
