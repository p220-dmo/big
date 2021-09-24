package org.ag.kafka;

import org.ag.kafka.config.AdminConfig;
import org.ag.kafka.util.admin.KafkaAdmin;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Created by ahmed.gater on 13/11/2016.
 */
public class KafkaAdminTest {
    String kafkaServerBootstrap = "192.168.99.100:9092";
    String zkConnectionString = "192.168.99.100:8121";
    String testTopic = "test";
    KafkaAdmin kafka;

    @Before
    public void setUp() throws Exception {
        kafka = new KafkaAdmin(new AdminConfig.KafkaConfigBuilder(kafkaServerBootstrap, zkConnectionString).build());
    }

    @Test
    public void createTopic() throws Exception {
        Assert.assertTrue(kafka.createTopic(testTopic, 1, 1));
        kafka.deleteTopic(testTopic);
    }

    @Test
    public void topics() throws Exception {
        kafka.createTopic(testTopic, 1, 1);
        Assert.assertEquals(kafka.topics().contains(testTopic), true);
        kafka.deleteTopic(testTopic);
    }

    @Test
    public void topicExists() throws Exception {
        kafka.createTopic(testTopic, 1, 1);
        Assert.assertEquals(kafka.topicExists(testTopic), true);
        kafka.deleteTopic(testTopic);
    }

    @Test
    public void topicInfo() {
        kafka.createTopic(testTopic, 1, 1);
        List<PartitionInfo> partitionInfos = kafka.topicInfo(testTopic);
        Assert.assertEquals(partitionInfos.size(), 1);
        Assert.assertEquals(partitionInfos.get(0).replicas().length, 1);
        kafka.deleteTopic(testTopic);
    }

}