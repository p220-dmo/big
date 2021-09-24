package org.ag.kafka.util.admin;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.ag.kafka.config.AdminConfig;
import org.ag.kafka.config.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

//import kafka.tools.GetOffsetShell

/**
 * Created by ahmed.gater on 12/11/2016.
 */
public class KafkaAdmin {
    AdminConfig config;

    public KafkaAdmin(AdminConfig kafkaCfg) {
        this.config = kafkaCfg;
    }


    public boolean createTopic(String topic, int partitions, int replication, Properties prop) {
        try {
            if (!topicExists(topic)) {
                AdminUtils.createTopic(zkUtil(), topic, partitions, replication, prop, RackAwareMode.Disabled$.MODULE$);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private ZkUtils zkUtil() {

        return new ZkUtils(zkClient(), new ZkConnection(this.config.getZkConnectionString()), this.config.isSecureKafkaCluster());
    }

    private ZkClient zkClient() {
        return new ZkClient(
                this.config.getZkConnectionString(),
                this.config.getSessionTimeoutMs(),
                this.config.getConnectionTimeoutMs(),
                ZKStringSerializer$.MODULE$);
    }


    public boolean createTopic(String topic, int partitions, int replication) {
        return createTopic(topic, partitions, replication, new Properties());
    }

    public boolean deleteTopic(String topic) {
        try {
            AdminUtils.deleteTopic(zkUtil(), topic);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public Set<String> topics() {
        try {
            return new HashSet(scala.collection.JavaConversions.seqAsJavaList(zkUtil().getAllTopics()));
        } catch (Exception e) {
            return null;
        }
    }

    public boolean topicExists(String topic) {
        return AdminUtils.topicExists(zkUtil(), topic);
    }

    public List<PartitionInfo> topicInfo(String topic) {
        Producer<String, String> producer = new KafkaProducer<String, String>
                (new ProducerConfig.ProducerConfigBuilder(this.config.getKafkaServerBootstrap(), topic).build().getProps());
        List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);
        producer.close();
        return partitionInfos;
    }


    public void topicPartitionOffset() {


    }
}