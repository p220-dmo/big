package dataframe.model;

import java.io.Serializable;

// This must be a JavaBean in order for Spark to infer a schema for it
//
public class Cust implements Serializable {
    private int id;
    private String name;
    private double sales;
    private double discount;
    private String state;

    public Cust(int id, String name, double sales, double discount, String state) {
        this.id = id;
        this.name = name;
        this.sales = sales;
        this.discount = discount;
        this.state = state;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getSales() {
        return sales;
    }

    public void setSales(double sales) {
        this.sales = sales;
    }

    public double getDiscount() {
        return discount;
    }

    public void setDiscount(double discount) {
        this.discount = discount;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}