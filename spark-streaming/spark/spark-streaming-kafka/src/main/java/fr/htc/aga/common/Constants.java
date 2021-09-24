package fr.htc.aga.common;

public interface Constants {
	
	//********************************************************************************
	//                        GLOBAL
	//********************************************************************************
	public final static String APPLICATION_NAME = "spark_kafka_test";
	public final static String ENVIRONNEMENT_NAME = "DEV";
	
	
	//********************************************************************************
	//                        REST API
	//********************************************************************************
	public final static String API_URL = "https://api.schiphol.nl/public-flights/flights";
	public final static String API_REST_ID = "ddf5a84d";
	public final static String API_REST_KEY = "cba9fc3b52ccc8e445ae7a01a8fc6157";
	public final static String FLIGHTS_SERVICE_PATH = "flights";
	public final static String CHARSET_ENCODING = "UTF-8";
	
	//********************************************************************************
	//                        KAFKA 
	//********************************************************************************
	public final static String KAFKA_BOOTSTRAP = "osboxes:6667";
	public final static String KAFKA_TOPIC_NAME = "test";
	public final static String KAFKA_CONSUMER_GROUP_ID = "kafka.spark.grp.id";
	
	//********************************************************************************
	//                        ZOOKEEPER 
	//********************************************************************************
	public final static String ZK_CONNECTION_STRING = "localhost:2181";
	public final static String ZK_PATH_SEPARATOR = "/";
	public final static String ZK_OFFSET_COMMIT_ROOT_PATH = "/spark-streaming-offsets";
	
	//********************************************************************************
	//                        SPARK 
	//********************************************************************************
	public static final String SPARK_MASTER_NAME = "local[2]";
}
