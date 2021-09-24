package dataframe.model;

import java.io.Serializable;

// A smaller JavaBean for a subset of the fields
    //
    public  class StateSales implements Serializable {
        private double sales;
        private String state;

        public StateSales(int id, String name, double sales, double discount, String state) {
            this.sales = sales;
            this.state = state;
        }

        public double getSales() {
            return sales;
        }

        public void setSales(double sales) {
            this.sales = sales;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }
    }
