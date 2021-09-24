package org.ag.hbase.util.crud;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Pair;
import org.javatuples.Quartet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by ahmed.gater on 23/11/2016.
 */
public class HBaseConsumer {

    Table table;
    Set<String> columnFamilies ;

    public HBaseConsumer(Table table) {
        try {
            this.table = table;
            this.columnFamilies = this.table.getTableDescriptor()
                                            .getFamilies()
                                            .stream()
                                            .map(HColumnDescriptor::getNameAsString)
                                            .collect(Collectors.toSet());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HBaseConsumer(Connection conn, String tablename) {
        try {
            table = conn.getTable(TableName.valueOf(tablename));
            this.columnFamilies = this.table.getTableDescriptor()
                    .getFamilies()
                    .stream()
                    .map(HColumnDescriptor::getNameAsString)
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public List<Quartet<String, String, String, String>> getRecords(String key, List<Pair<String, String>> famQuals) {
        Get get = new Get(key.getBytes());
        for (Pair<String, String> fq : famQuals) {
            if (this.columnFamilies.contains(fq.getFirst())){
                get = get.addColumn(fq.getFirst().getBytes(), fq.getSecond().getBytes());
            }
        }
        try {
            return processResults(this.table.get(get));
        } catch (Exception e) {
            return null;
        }
    }

    public Quartet<String, String, String, String> getRecord(String key, String family, String qualifier) {
        if (!this.columnFamilies.contains(family)){
            return null ;
        }
        List<Pair<String, String>> famQuals = new ArrayList<>();
        famQuals.add(new Pair<>(family, qualifier));
        try {
            return getRecords(key, famQuals).get(0);
        } catch (Exception e) {
            return null;
        }
    }

    public List<Quartet<String, String, String, String>> scan(String startKey,
                                                              String endKey,
                                                              List<Pair<String, String>> famQuals,
                                                              List<Filter> filters) {
        Scan s = new Scan(startKey.getBytes(),endKey.getBytes()) ;
        //adding columns to the scan
        if (famQuals != null){
            for (Pair<String, String> fq : famQuals) {
                if (this.columnFamilies.contains(fq.getFirst())){
                    s = s.addColumn(fq.getFirst().getBytes(), fq.getSecond().getBytes());
                }
            }
        }
        // adding filters to the scan
        if (filters != null){
            for(Filter f: filters){
                s = s.setFilter(f) ;
            }
        }
        try {
            return processResultScanner(table.getScanner(s));
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.<Quartet<String, String, String, String>>emptyList() ;
        }

    }
    private List<Quartet<String, String, String, String>> processResultScanner(ResultScanner scanner){
        List<Quartet<String, String, String, String>> results = new ArrayList<>() ;
        try  {
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                results.addAll(processResults(rr));
            }
            return results ;
        } catch (IOException e) {
            e.printStackTrace();
            results = Collections.<Quartet<String, String, String, String>>emptyList() ;
        }
        return results ;
    }
    private List<Quartet<String, String, String, String>> processResults(Result result) {
        CellScanner scanner = result.cellScanner();
        List<Quartet<String, String, String, String>> qrts = new ArrayList<>();
        try {
            while (scanner.advance()) {
                Cell cell = scanner.current();
                qrts.add(new Quartet<>(new String(CellUtil.cloneRow(cell), StandardCharsets.UTF_8),
                        new String(CellUtil.cloneFamily(cell), StandardCharsets.UTF_8),
                        new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8),
                        new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return qrts;
    }
}
