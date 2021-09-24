package org.ag.hbase.util.admin;

import com.google.common.collect.ImmutableList;
import org.ag.hbase.conf.ColumnFamilyConfig;
import org.ag.hbase.conf.HBaseConnectionBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by ahmed.gater on 16/11/2016.
 */
public class AdminUtilsTest {
    //private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    @Test
    public void createTableTest() throws Exception {
        AdminUtils admUtil = new AdminUtils(new HBaseConnectionBuilder("localhost:2181").build());
        admUtil.createTable("default", "test2", ImmutableList.of("cf1", "cf2"), new ColumnFamilyConfig());
        Assert.assertTrue(admUtil.tables().contains("test2"));
        admUtil.deleteTable("default", "test2");
    }


    @Test
    public void deleteTableTest() throws Exception {
        AdminUtils admUtil = new AdminUtils(new HBaseConnectionBuilder("localhost:2181").build());
        admUtil.createTable("default", "test2", ImmutableList.of("cf1", "cf2"), new ColumnFamilyConfig());
        admUtil.deleteTable("default", "test2");
        Assert.assertFalse(admUtil.tables().contains("test2"));
    }

}