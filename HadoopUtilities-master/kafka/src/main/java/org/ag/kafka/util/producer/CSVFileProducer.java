package org.ag.kafka.util.producer;

import org.ag.kafka.config.ProducerConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.FileReader;
import java.io.IOException;



/**
 * Created by ahmed.gater on 28/11/2016.
 */
public class CSVFileProducer {
    CSVParser parser ;
    ProducerConfig prodConfig;
    SimpleProducer<String, String> producer;

    public CSVFileProducer(CSVParser parser,ProducerConfig prodCfg){
        this.parser = parser ;
        this.prodConfig = prodCfg;
        this.producer = new SimpleProducer<String, String>(prodConfig);
    }

    public CSVFileProducer(String filePath, char fieldDelimiter){
        try {
            parser = new CSVParser( new FileReader(filePath),
                    CSVFormat.DEFAULT.withDelimiter(fieldDelimiter).withHeader());

        } catch (IOException e  ){
            e.printStackTrace();
        }
    }

    public void sendAll() throws IOException {
        this.parser.forEach(x ->
        {
            System.out.println(x.toMap().toString());
            this.producer.send(x.toMap().toString(), x.toMap().toString());
            System.out.println(x.toMap().toString());
        }) ;
    }


    public static void main(String[] args) throws IOException {

        if (args.length <4){
            System.out.println("");
            return ;
        }

        String filePath = args[0] ; //"C:/Users/ahmed.gater/Documents/data/processmining/process_data.csv" ;
        char delimiter = args[1].charAt(0) ; // ';'
        String kafka_bootstrap = args[2] ; //"localhost:9092"
        String topic = args[3] ; //"spark-streaming-test"

        CSVParser parser;
        parser = new CSVParser( new FileReader(filePath),
                CSVFormat.DEFAULT.withDelimiter(delimiter).withHeader());


        ProducerConfig cfg = new ProducerConfig.ProducerConfigBuilder(kafka_bootstrap, topic).build();

        CSVFileProducer prod = new CSVFileProducer(parser,cfg) ;
        prod.sendAll();

    }
}
