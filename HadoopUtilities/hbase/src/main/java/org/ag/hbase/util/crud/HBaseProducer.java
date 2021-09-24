package org.ag.hbase.util.crud;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ahmed.gater on 23/11/2016.
 */
public class HBaseProducer {
    Table table;
    int batchSize = 10;

    List<Put> waitingPuts;

    public HBaseProducer(Connection conn, String tablename, int batchSize) {

        this.batchSize = batchSize;
        try {
            table = conn.getTable(TableName.valueOf(tablename));
             waitingPuts = new ArrayList<>();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Adding a new record to hbase
     */
    public void addRecord(String rowKey, String family, String qualifier, String value) throws Exception {
        this.waitingPuts.add(
                new Put(Bytes.toBytes(rowKey))
                        .addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes()));
        if (this.waitingPuts.size() == this.batchSize){
            this.table.put(this.waitingPuts);
            this. waitingPuts = new ArrayList<>();
        }
    }

    public boolean flush(){
        try {
            if (this.waitingPuts.size()>0){
                this.table.put(this.waitingPuts) ;
            }
            this. waitingPuts = new ArrayList<>();
            return true ;
        } catch (IOException e) {
            e.printStackTrace();
            return false ;
        }
    }
}
