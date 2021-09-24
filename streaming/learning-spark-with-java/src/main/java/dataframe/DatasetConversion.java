package dataframe;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import dataframe.model.Cust;
import dataframe.model.StateSales;

//
// Explore interoperability between DataFrame and Dataset. Note that Dataset
// is covered in much greater detail in the 'dataset' directory.
//
public class DatasetConversion {

    
    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("DataFrame-DatasetConversion")
            .master("local[4]")	
            .getOrCreate();

        //
        // The Java API requires you to explicitly instantiate an encoder for
        // any JavaBean you want to use for schema inference
        //
        Encoder<Cust> custEncoder = Encoders.bean(Cust.class);
        //
        // Create a container of the JavaBean instances
        //
        List<Cust> data = Arrays.asList(
                new Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
                new Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
                new Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
                new Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
                new Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
        );
        //
        // Use the encoder and the container of JavaBean instances to create a
        // Dataset
        //
        Dataset<Cust> ds = spark.createDataset(data, custEncoder);

        System.out.println("*** here is the schema inferred from the Cust bean");
        ds.printSchema();

        System.out.println("*** here is the data");
        ds.show();

        //
        // Querying a Dataset of any type results in a
        // DataFrame (i.e. Dastaset<Row>)
        //

        Dataset<Row> smallerDF =
                ds.select("sales", "state").filter(col("state").equalTo("CA"));

        System.out.println("*** here is the dataframe schema");
        smallerDF.printSchema();

        System.out.println("*** here is the data");
        smallerDF.show();

        //
        // But a Dataset<Row> can be converted back to a Dataset of some other
        // type by using another bean encoder
        //

        Encoder<StateSales> stateSalesEncoder = Encoders.bean(StateSales.class);

        Dataset<StateSales> stateSalesDS = smallerDF.as(stateSalesEncoder);

        System.out.println("*** here is the schema inferred from the StateSales bean");
        stateSalesDS.printSchema();

        System.out.println("*** here is the data");
        stateSalesDS.show();

        spark.stop();
    }
}
