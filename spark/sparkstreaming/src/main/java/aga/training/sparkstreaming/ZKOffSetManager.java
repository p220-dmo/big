package sparkstreaming.src.main.java.aga.training.sparkstreaming;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/*
Class used to handle offset of Spark Streaming Jobs
 */
public class ZKOffSetManager {
    private static String ZPATH_SEPARATOR = "/" ;
    private ZooKeeper zkeeper;
    private String commitRootPath;
    private String consumerGrpPath;
    private String consumerGrp;

    // connectionString="host1:port1,host2:port2"
    public ZKOffSetManager(String connectionString, String commitRootPath, String consumerGrp) throws IOException, InterruptedException, KeeperException {
        this.zkeeper = new ZooKeeper(connectionString, 2000000, null);
        this.commitRootPath = commitRootPath;
        this.consumerGrp = consumerGrp;
        this.consumerGrpPath = this.commitRootPath + ZPATH_SEPARATOR + this.consumerGrp ;
        this.initilize();
    }

    private void initilize() throws KeeperException, InterruptedException {
        Stat rootPathExists = zkeeper.exists(this.commitRootPath, false);
        if (rootPathExists == null){
            this.createZNode(this.commitRootPath, null);
            this.createZNode(this.consumerGrpPath, null) ;
        }
        else{
            Stat consumerGrpPathExists = zkeeper.exists(this.consumerGrpPath, false);
            if (consumerGrpPathExists == null){
                this.createZNode(this.consumerGrpPath, null) ;
            }
        }
    }

    private boolean ZNExists(String path) throws KeeperException, InterruptedException {
        return (zkeeper.exists(path, false) != null) ;
    }
    private void createZNode(String path,byte[] data) throws KeeperException, InterruptedException {
        byte[] realData = new byte[0];
        int pos=1;
        do {
            pos=path.indexOf(ZPATH_SEPARATOR,pos + 1);
            if (pos == -1) {
                pos=path.length();
                realData = data ;
            }
            String subPath=path.substring(0,pos);
            if (this.zkeeper.exists(subPath,false) == null) {
                this.zkeeper.create(subPath,realData,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
        }
        while (pos < path.length());
    }

    private Object getZNodeData(String path, boolean watchFlag) throws KeeperException,InterruptedException, UnsupportedEncodingException {
        byte[] b = null;
        b = zkeeper.getData(path, null, null);
        return new String(b, "UTF-8");
    }

    private void updateOrCreate(String path, byte[] data) throws KeeperException,InterruptedException {
        Stat rootPathExists = zkeeper.exists(path, false);
        if (rootPathExists == null){
            this.createZNode(path,data);
        }
        else{
            int version = rootPathExists.getVersion();
            zkeeper.setData(path, data, version);
        }
    }


    /*
    Commiting the offsets to ZK
     */
    public void commitOffset(OffsetRange[] offsetRanges) throws KeeperException, InterruptedException {
        for (OffsetRange o : offsetRanges){
            this.updateOrCreate(
                    this.consumerGrpPath + ZPATH_SEPARATOR  + o.topic()  + ZPATH_SEPARATOR + o.partition(),
                    String.valueOf(o.untilOffset()).getBytes()
            );
        }
    }

    /*
    Reading the commited offsets
     */
    public Map<TopicPartition, Long> getCommitedOffset() throws KeeperException, InterruptedException, UnsupportedEncodingException {
        String rootP = this.consumerGrpPath  ;
        Map<TopicPartition, Long> startingOffset = new HashMap<>() ;
        if (!this.ZNExists(rootP)){
            return null ;
        }
        List<String> topics = zkeeper.getChildren(rootP, false);
        for(String topic:topics){
            String topicPath = rootP + ZPATH_SEPARATOR + topic ;
            List<String> partitions = zkeeper.getChildren(topicPath, false);
            for(String partition: partitions){
                Long fromOffset = Long.valueOf(String.valueOf(this.getZNodeData(topicPath + ZPATH_SEPARATOR + partition, false)));
                TopicPartition tp = new TopicPartition(topic, Integer.valueOf(partition)) ;
                startingOffset.put(tp,fromOffset) ;
            }
        }
       return startingOffset ;
    }

    public static void main (String[] args) throws InterruptedException, IOException, KeeperException {
        String consumerGrp = "SparkStreamingTraining-Grp";
        ZKOffSetManager zkOffSetManager = new ZKOffSetManager("localhost:2181",
                "/spark-streaming-offsets", consumerGrp);

        Map<TopicPartition, Long> offsets = zkOffSetManager.getCommitedOffset();
        System.out.println(offsets) ;
    }

    }

