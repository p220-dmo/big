package fr.htc.aga.rest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import fr.htc.aga.common.Constants;

import static fr.htc.aga.common.Constants.API_REST_ID;
import static fr.htc.aga.common.Constants.API_REST_KEY;
import static fr.htc.aga.common.Constants.KAFKA_BOOTSTRAP;
import static fr.htc.aga.common.Constants.KAFKA_TOPIC_NAME;

import java.util.List;
import java.util.Properties;

/*
Class used to populate Kafka topic with the fetched flight data to be used in the spark streaming job
 */
public class FlightKafkaProducer {
    KafkaProducer<String, String> producer ;
    String topic ;

    /**
     * Build the Kafka producer object and set up the topic name
     * @param bootstrap
     * @param topic
     */
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

    /**
     * This method send all messages in collection param
     * @param events
     */
    public void send(List<String> events){
        events.forEach( e -> {
                this.producer.send(new ProducerRecord<String, String>(topic, e), (recordMetadata, e1) -> {
                    if (e1 == null){
                        System.out.format(" Sending message with offset: %s and partition %d.\n", recordMetadata.offset(), recordMetadata.partition()) ;
                    }
                    else{
                        System.out.println("error");
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
    	
        FlightInfoFetcher fInfoFetcher = new FlightInfoFetcher(API_REST_ID, API_REST_KEY) ;
        FlightKafkaProducer producer = new FlightKafkaProducer(KAFKA_BOOTSTRAP , KAFKA_TOPIC_NAME) ;

        while(true){
            producer.send(fInfoFetcher.getFlights());
            Thread.sleep(10000);
        }
    }
}