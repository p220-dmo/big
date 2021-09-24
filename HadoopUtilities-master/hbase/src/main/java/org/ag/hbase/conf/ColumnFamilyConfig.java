package org.ag.hbase.conf;

import lombok.Getter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;

/**
 * Created by ahmed.gater on 16/11/2016.
 */
@Getter
public class ColumnFamilyConfig {

    private Compression.Algorithm compressionAlgo = Compression.Algorithm.GZ;
    private BloomType bloom = BloomType.ROWCOL;
    private int minVersion = 1;
    private int maxVersion = 5;

    public ColumnFamilyConfig() {

    }

    public ColumnFamilyConfig versions(int min, int max) {
        this.minVersion = min;
        this.maxVersion = max;
        return this;
    }

    public ColumnFamilyConfig compression(Compression.Algorithm ca) {
        this.compressionAlgo = ca;
        return this;
    }

    public ColumnFamilyConfig bloomFilter(BloomType bt) {
        this.bloom = bt;
        return this;
    }

}




