package sparkstreaming.src.main.java.aga.training.sparkstreaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

/*
Class used to populate Kafka topic with the fetched flight data to be used in the spark streaming job
 */
public class FlightKafkaProducer {
    KafkaProducer<String, String> producer ;
    String topic ;

    public FlightKafkaProducer(String bootstrap, String topic){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
        this.topic = topic ;
    }


    public void send(List<String> events){
        events.forEach( e -> {
                this.producer.send(new ProducerRecord(topic, e), (recordMetadata, e1) -> {
                    if (e == null){
                        System.out.println("Error when sending record: " + e) ;
                    }
                    else{
                        // No thing to do
                    }
                });
            }
        );
        System.out.println("Number of records sent: " + events.size()) ;
    }

    public void close(){
        this.producer.flush();
        this.producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        String appId = "ddf5a84d";
        String appKey = "cba9fc3b52ccc8e445ae7a01a8fc6157";
        String kafkaBootstrap = "aga.hdp:6667" ;
        String topic = "sparkstreamingtest" ;

        FlightInfoFetcher fInfoFetcher = new FlightInfoFetcher(appId,appKey) ;
        FlightKafkaProducer producer = new FlightKafkaProducer(kafkaBootstrap,topic ) ;

        while(true){
            producer.send(fInfoFetcher.getFlights());
            Thread.sleep(10000);
        }
    }




}