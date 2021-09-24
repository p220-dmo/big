package org.ag.kafka.util.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Created by ahmed.gater on 15/11/2016.
 */
public class BasicRecordHandler<K,V> implements RecordHandler<String, String> {

    @Override
    public boolean process(ConsumerRecords<String, String> l) {
        l.forEach(x-> System.out.println(x.value()));
        return true;
    }
}
