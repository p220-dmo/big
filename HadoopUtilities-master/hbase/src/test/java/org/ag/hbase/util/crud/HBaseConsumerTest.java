package org.ag.hbase.util.crud;

import org.ag.hbase.conf.HBaseConnectionBuilder;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.javatuples.Quartet;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ahmed.gater on 27/11/2016.
 */
public class HBaseConsumerTest {
    @Test
    public void getRecords() throws Exception {
        HBaseConsumer g = new HBaseConsumer(new HBaseConnectionBuilder("quickstart.cloudera:2181").build(), "t1");
        List<Pair<String, String>> famQuals = new ArrayList<>();
        famQuals.add(new Pair<>("cf1", "col1"));
        Assert.assertEquals(g.getRecords("key1",famQuals).get(0).getValue1(),"cf1");
    }

    @Test
    public void getRecord() throws Exception {
        HBaseConsumer g = new HBaseConsumer(new HBaseConnectionBuilder("quickstart.cloudera:2181").build(), "t1");
        List<Pair<String, String>> famQuals = new ArrayList<>();
        famQuals.add(new Pair<>("cf1", "col1"));
        String[] rep = {"key1", "cf1", "col1", "value"} ;
        Assert.assertEquals(g.getRecord("key1","cf1","col1").toString(),"[key1, cf1, col1, value]");
    }

    @Test
    public void scan() throws Exception {
        HBaseConsumer g = new HBaseConsumer(new HBaseConnectionBuilder("quickstart.cloudera:2181").build(), "t1");
        List<Filter> filters = new ArrayList<>() ;
        ColumnPrefixFilter colFil = new ColumnPrefixFilter(Bytes.toBytes("col")) ;
        filters.add(colFil);
        List<Pair<String, String>> famQuals = new ArrayList<>() ;
        famQuals.add(new Pair<>("cf1","col1"));
        List<Quartet<String, String, String, String>> res = g.scan("k", "l",famQuals,filters);

        Assert.assertEquals(res.size(),1);
        Assert.assertEquals(res.get(0).getValue1().toString(),"cf1");
    }

}