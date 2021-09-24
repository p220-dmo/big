package org.ag.hbase.util.admin;

import org.ag.hbase.conf.ColumnFamilyConfig;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by ahmed.gater on 16/11/2016.
 */
public class AdminUtils {
    Connection hbaseConnection;
    Admin hbaseAdmin;

    public AdminUtils(Connection hbaseConn) throws IOException {
        this.hbaseConnection = hbaseConn;
        this.hbaseAdmin = this.hbaseConnection.getAdmin();
    }

    public boolean createTable(String nameSpace, String name, List<String> columnFamilies, ColumnFamilyConfig cfConf) {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(nameSpace, name));
        columnFamilies.stream().forEach(x -> desc.addFamily(buildColumnFamily(x, cfConf)));
        try {
            this.hbaseAdmin.createTable(desc);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private HColumnDescriptor buildColumnFamily(String cfNale, ColumnFamilyConfig cgBuilder) {
        return new HColumnDescriptor(cfNale)
                .setVersions(cgBuilder.getMinVersion(), cgBuilder.getMaxVersion())
                .setBloomFilterType(cgBuilder.getBloom())
                .setCompactionCompressionType(cgBuilder.getCompressionAlgo());

    }

    public boolean deleteTable(String namespace, String name) {
        try {
            this.hbaseAdmin.disableTable(TableName.valueOf(namespace, name));
            this.hbaseAdmin.deleteTable(TableName.valueOf(namespace, name));
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public Set<String> tables() {
        try {
            return Arrays.stream(this.hbaseAdmin.listTableNames())
                    .map(x -> x.getNameAsString())
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            return null;
        }
    }

    public Set<String> columnFamilies(String tableName) {
        try {
            return Arrays.stream(this.hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName))
                    .getColumnFamilies())
                    .map(x -> x.getNameAsString())
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
