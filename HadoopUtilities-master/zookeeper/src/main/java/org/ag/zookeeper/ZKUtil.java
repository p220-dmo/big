package org.ag.zookeeper;

import com.google.common.base.Joiner;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * Created by ahmed.gater on 12/11/2016.
 */
public class ZKUtil {
    private ZooKeeper zk;

    /*
    connectionString = "zkhost1,zkHost2,...,zkHostn:port"
     */
    public ZKUtil(String connectoinString) throws IOException {
        initZK(connectoinString);
    }

    public ZKUtil(List<String> hosts, int port) throws IOException {
        initZK(buildConnectionString(hosts,port));
    }

    private void initZK(String connectionString) throws IOException{
        this.zk = new ZooKeeper(connectionString,5000,null) ;
    }

    public String buildConnectionString(List<String> hosts, int port) {
        return Joiner.on(",").join(hosts).concat(":").concat(String.valueOf(port)) ;
    }

    /*
    Disconnecting from zookeeper server
     */
    public void close() throws InterruptedException {
        zk.close();
    }

    // Method to check existence of znode and its status, if znode is available.
    public boolean zNodeExists(String path) {
        try{
            return zk.exists(path, true) != null ;
        }
        catch (Exception e){
            return false ;
        }
    }

    public Stat zNodeStatus(String path){
        try{
            return zk.exists(path,true) ;
        }
        catch (Exception e){
            return null ;
        }
    }

    public boolean createZNode(String path, byte[] data)  {
        try{
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            return true ;
        }
        catch(Exception e){
            return false ;
        }
    }

    public byte[] getZNodeData(String path) throws InterruptedException, KeeperException {
        try {
            Stat stat = zNodeStatus(path) ;
            if(stat != null) {
                return zk.getData(path,false, null);
            } else {
                return null ;
            }
        } catch(Exception e) {
            return null;
        }
    }

    public boolean updateZNode(String path, byte[] data) {
        try{
            zk.setData(path, data, zNodeStatus(path).getVersion());
            return true ;
        }
        catch(Exception e){
            return false;
        }
    }

    public boolean deleteZNode(String path){
        try {
            this.zk.delete(path, zNodeStatus(path).getVersion());
            return true ;
        }catch (Exception e){
            return false;
        }
    }


}
