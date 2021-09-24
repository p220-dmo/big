package fr.htc.aga.sparkstreaming;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.apache.zookeeper.KeeperException;

public class SparkStreamingTraining {

	/*
	 * Input Streaming reading kafka topic from the earliest offset
	 */
	private static JavaInputDStream<ConsumerRecord<String, String>> buildStreamFromEarliestOffset(
			JavaStreamingContext jssc, String kafkaBootstrap, String topic, String consumerGrp) {

		Map<String, Object> kafkaParams = new HashMap();

		kafkaParams.put("bootstrap.servers", kafkaBootstrap);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", consumerGrp);
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", false);

		return KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(Arrays.asList(topic), kafkaParams));
	}

	

	public static void handleRDDStream(RDD<ConsumerRecord<String, String>> streamRDD) {
		// Do what ever you want with the RDD
	}

	public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
		String env = "DEV";
		String appName = "test";
		int duration = 30;
		String kafkaBootstrap = "aga.hdp:6667";
		String topic = "sparkstreamingtest";
		String consumerGrp = "aga.training.sparkstreaming.SparkStreamingTraining-Grp";
		String zkConnectionString = "localhost:2181";
		String zkOffsetCommitRootPath = "/spark-streaming-offsets";
		ZKOffSetManager zkOffSetManager = new ZKOffSetManager(zkConnectionString, zkOffsetCommitRootPath, consumerGrp);

		JavaStreamingContext jssc = buildSparkStreamingContext(env, appName, duration);
		
		JavaInputDStream<ConsumerRecord<String, String>> stream = buildStreamFromEarliestOffset(jssc, kafkaBootstrap,
				topic, consumerGrp);

		stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
			@Override
			public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD) throws Exception {
				RDD<ConsumerRecord<String, String>> rdd = consumerRecordJavaRDD.rdd();
				handleRDDStream(rdd);
				// Retrieving the offsets of the RDD
				OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd).offsetRanges();

				// Once RDD handled, offset are commited to ZK
				zkOffSetManager.commitOffset(offsetRanges);
			}
		});

		jssc.start();
		jssc.awaitTermination();
	}
	public static JavaStreamingContext buildSparkStreamingContext(String env, String appName, int duration) {
		SparkConf conf = new SparkConf().setAppName(appName);
		if (env.equals("DEV")) {
			conf = conf.setMaster("local[2]");
		}
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(duration));
		return jssc;
	}

}
