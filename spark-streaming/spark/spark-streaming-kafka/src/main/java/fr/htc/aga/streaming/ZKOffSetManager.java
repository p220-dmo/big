package fr.htc.aga.streaming;

import static fr.htc.aga.common.Constants.CHARSET_ENCODING;
import static fr.htc.aga.common.Constants.KAFKA_CONSUMER_GROUP_ID;
import static fr.htc.aga.common.Constants.ZK_CONNECTION_STRING;
import static fr.htc.aga.common.Constants.ZK_OFFSET_COMMIT_ROOT_PATH;
import static fr.htc.aga.common.Constants.ZK_PATH_SEPARATOR;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import fr.htc.aga.common.Constants;


/*
Class used to handle offset of Spark Streaming Jobs
 */
public class ZKOffSetManager {
    private ZooKeeper zkeeper;
    private String commitRootPath;
    private String consumerGrpPath;
    private String consumerGrp;

    // connectionString="host1:port1,host2:port2"
    public ZKOffSetManager(String connectionString, String commitRootPath, String consumerGrp) throws IOException, InterruptedException, KeeperException {
        this.zkeeper = new ZooKeeper(connectionString, 2000000, null);
        this.commitRootPath = commitRootPath;
        this.consumerGrp = consumerGrp;
        this.consumerGrpPath = this.commitRootPath + ZK_PATH_SEPARATOR + this.consumerGrp ;
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
            pos=path.indexOf(ZK_PATH_SEPARATOR,pos + 1);
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
        return new String(b, CHARSET_ENCODING);
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
                    this.consumerGrpPath + ZK_PATH_SEPARATOR  + o.topic()  + ZK_PATH_SEPARATOR + o.partition(),
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
            String topicPath = rootP + ZK_PATH_SEPARATOR + topic ;
            List<String> partitions = zkeeper.getChildren(topicPath, false);
            for(String partition: partitions){
                Long fromOffset = Long.valueOf(String.valueOf(this.getZNodeData(topicPath + ZK_PATH_SEPARATOR + partition, false)));
                TopicPartition tp = new TopicPartition(topic, Integer.valueOf(partition)) ;
                startingOffset.put(tp,fromOffset) ;
            }
        }
       return startingOffset ;
    }

    public static void main (String[] args) throws InterruptedException, IOException, KeeperException {
        ZKOffSetManager zkOffSetManager = new ZKOffSetManager(ZK_CONNECTION_STRING, ZK_OFFSET_COMMIT_ROOT_PATH, KAFKA_CONSUMER_GROUP_ID);

        Map<TopicPartition, Long> offsets = zkOffSetManager.getCommitedOffset();
        System.out.println(offsets) ;
    }

    }

