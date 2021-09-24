package org.ag.hbase.util.admin;

import org.ag.hbase.conf.HBaseConfigBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by ahmed.gater on 16/11/2016.
 */
public class HBaseConnectionBuilderTest {

    @Test
    public void extractPort() throws Exception {
        HBaseConfigBuilder conn = new HBaseConfigBuilder("localhost:2181");
        Assert.assertEquals(conn.extractPort("localhost:2181"), "2181");
    }

    @Test
    public void extractQuorum() throws Exception {
        HBaseConfigBuilder conn = new HBaseConfigBuilder("localhost:2181");
        Assert.assertEquals(conn.extractQuorum("localhost:2181"), "localhost");
    }

}