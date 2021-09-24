package fr.htc.spark.beans;

import java.io.Serializable;

public class Store implements Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 6925572279458666523L;

        private Integer storeId;
        private Integer regionId;

        public Integer getStoreId() {
                return storeId;
        }

        public void setStoreId(Integer storeId) {
                this.storeId = storeId;
        }

        public Integer getRegionId() {
                return regionId;
        }

        public void setRegionId(Integer regionId) {
                this.regionId = regionId;
        }

        public static Store parse(String line) {

                String[] columns = line.split(";");
                Store store = new Store();

                store.setRegionId(Integer.parseInt(columns[0]));
                store.setStoreId(Integer.parseInt(columns[2]));
                
                return store;
        
        }
                
                
                public String toString() {
                        return "Store [storeId=" + storeId + ", region_id=" + regionId + "]";
                }
        

}