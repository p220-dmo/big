package org.ag.hbase.conf;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by ahmed.gater on 16/11/2016.
 */
public class HBaseConfigBuilder {
    Configuration conf;

    public HBaseConfigBuilder(String zkConnectionString) {
        this.conf = new Configuration();
        this.conf.set("hbase.zookeeper.quorum", extractQuorum(zkConnectionString));
        this.conf.set("hbase.zookeeper.property.clientPort", extractPort(zkConnectionString));
        this.conf.set("hbase.master", "localhost:60010");
    }

    public HBaseConfigBuilder sessionTimeOut(int timeout) {
        this.conf.setInt("timeout", timeout);
        return this;
    }

    public Configuration config() throws IOException {
        return this.conf;
    }

    public String extractPort(String zkConnectionString) {
        return zkConnectionString.split(":")[1];
    }

    public String extractQuorum(String zkConnectionString) {
        return zkConnectionString.split(":")[0];
    }
}
