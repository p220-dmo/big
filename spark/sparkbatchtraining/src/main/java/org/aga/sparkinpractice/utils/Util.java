package org.aga.sparkinpractice.utils;

import org.aga.sparkinpractice.model.Sale;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Util {
    public static Map<String, DataType> JAVATYPETOSPARKSQLMAPPING = Stream.of(
            new AbstractMap.SimpleEntry<>("short", DataTypes.ShortType),
            new AbstractMap.SimpleEntry<>("int", DataTypes.IntegerType),
            new AbstractMap.SimpleEntry<>("long", DataTypes.LongType),
            new AbstractMap.SimpleEntry<>("double", DataTypes.DoubleType),
            new AbstractMap.SimpleEntry<>("float", DataTypes.FloatType),
            new AbstractMap.SimpleEntry<>("boolean", DataTypes.BooleanType),
            new AbstractMap.SimpleEntry<>("java.lang.Byte", DataTypes.ByteType),
            new AbstractMap.SimpleEntry<>("byte", DataTypes.ByteType),
            new AbstractMap.SimpleEntry<>("java.sql.Timestamp",DataTypes.TimestampType),
            new AbstractMap.SimpleEntry<>("java.sql.Date",DataTypes.DateType),
            new AbstractMap.SimpleEntry<>("java.lang.String",DataTypes.StringType)
    ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)) ;

    public static Map<String, Object> rowToMap(Row r) {
        List<String> fields = Arrays.asList(r.schema().fields()).stream().map(f -> f.name()).collect(Collectors.toList());
        scala.collection.immutable.Map<String, Object> valuesMap = r.getValuesMap(
                JavaConverters.asScalaIteratorConverter(fields.iterator()).asScala().toSeq());

        Map<String, Object> rowAsMap = new HashMap();
        Iterator<Tuple2<String, Object>> it = valuesMap.iterator();
        while (it.hasNext()) {
            Tuple2<String, Object> next = it.next();
            rowAsMap.put(next._1(), next._2());
        }
        return rowAsMap;
    }

    public static StructType buildSchema(Class cls) throws Exception {
        Field[] fields = cls.getDeclaredFields();
        List<StructField> structFields = new ArrayList<>();
        for (Field field : fields) {
            if (!field.isSynthetic() && !Modifier.isStatic(field.getModifiers())) {
                String name = field.getName();
                DataType dfType = JAVATYPETOSPARKSQLMAPPING.get(field.getType().getCanonicalName()) ;
                if (dfType ==null)
                    throw new Exception("Type not supported: " + field.getType().getCanonicalName()) ;
                structFields.add(DataTypes.createStructField(name, dfType, true));
            }
        }
        return DataTypes.createStructType(structFields);
    }

    public static Dataset<Row> loadAsDF(SparkSession sparkSession, String saleFilePath,Class cls) throws Exception {
        return sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Util.buildSchema(cls))
                .load(saleFilePath)
                ;
    }

    public static <T> Dataset loadAsDS(SparkSession sparkSession, String saleFilePath,Class cls) throws Exception {
        return sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Util.buildSchema(cls))
                .load(saleFilePath)
                .as(Encoders.bean(cls))
         ;
    }

    public static void main(String[] args) throws Exception {
        StructType structType = buildSchema(Sale.class);
        structType.printTreeString();

    }
}
