package org.ag.kafka.util.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Created by ahmed.gater on 14/11/2016.
 */
public interface RecordHandler<K,V>   {
    public boolean process(ConsumerRecords<K, V> l) ;
}
