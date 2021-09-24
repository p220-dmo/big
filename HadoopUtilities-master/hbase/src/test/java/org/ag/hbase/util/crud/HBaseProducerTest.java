package org.ag.hbase.util.crud;

import com.google.common.collect.ImmutableList;
import org.ag.hbase.conf.ColumnFamilyConfig;
import org.ag.hbase.conf.HBaseConnectionBuilder;
import org.ag.hbase.util.admin.AdminUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ahmed.gater on 08/12/2016.
 */
public class HBaseProducerTest {
    String tableName  = "test2" ;
    AdminUtils admUtil ;
    HBaseProducer hbaseProd ;
    String key = "key" ;
    String colFam = "cf1" ;
    String col = "col" ;
    String val = "val" ;
    String zooKeeperConnString = "quickstart.cloudera:2181" ; //"localhost:2181" ;
    @Before
    public void setUp() throws Exception {
        admUtil = new AdminUtils(new HBaseConnectionBuilder(zooKeeperConnString).build());
        admUtil.createTable("default", tableName, ImmutableList.of(colFam), new ColumnFamilyConfig());
        hbaseProd = new HBaseProducer(new HBaseConnectionBuilder(zooKeeperConnString).build(), tableName,10);
    }

    @Test
    public void addRecord() throws Exception {
        hbaseProd = new HBaseProducer(new HBaseConnectionBuilder(zooKeeperConnString).build(), tableName,10);
        hbaseProd.addRecord(key,colFam,col,val);
        hbaseProd.flush() ;
        HBaseConsumer g = new HBaseConsumer(new HBaseConnectionBuilder(zooKeeperConnString).build(), tableName);
        List<Pair<String, String>> famQuals = new ArrayList<>();
        famQuals.add(new Pair<>(colFam, col));
        String rep = "[" + key + ", " + colFam + ", " + col + ", " + val + "]" ;
        Assert.assertEquals(g.getRecord(key,colFam,col).toString(),rep);
    }

     @After
    public void t(){
        boolean b = admUtil.deleteTable(tableName);
    }
}