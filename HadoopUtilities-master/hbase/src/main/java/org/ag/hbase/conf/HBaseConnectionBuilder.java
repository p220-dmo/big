package org.ag.hbase.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by ahmed.gater on 16/11/2016.
 */
public class HBaseConnectionBuilder {
    Configuration conf;

    public HBaseConnectionBuilder(String zkConnectionString) {
        this.conf = new Configuration();
        this.conf.set("hbase.zookeeper.quorum", extractQuorum(zkConnectionString));
        this.conf.set("hbase.zookeeper.property.clientPort", extractPort(zkConnectionString));
    }

    public HBaseConnectionBuilder sessionTimeOut(int timeout) {
        this.conf.setInt("timeout", timeout);
        return this;
    }

    public Connection build() throws IOException {
        return ConnectionFactory.createConnection(conf);
    }

    public String extractPort(String zkConnectionString) {
        return zkConnectionString.split(":")[1];
    }

    public String extractQuorum(String zkConnectionString) {
        return zkConnectionString.split(":")[0];
    }
}
