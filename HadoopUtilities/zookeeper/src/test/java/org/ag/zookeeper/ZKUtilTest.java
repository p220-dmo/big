package org.ag.zookeeper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by ahmed.gater on 12/11/2016.
 */
public class ZKUtilTest {
    ZKUtil zkU ;
    @Before
    public void setUp() throws Exception {
        String connectionString = "localhost:2181" ;
        zkU = new ZKUtil(connectionString) ;
    }

    @Test
    public void connectionString(){
        assertEquals(zkU.buildConnectionString(Arrays.asList("serv1","serv2"), 2181),"serv1,serv2:2181");
    }
    @Test
    public void zNodeDoesNotExist() throws Exception {
        String path = "/test" ;
        zkU.deleteZNode(path);
        assertFalse(zkU.zNodeExists(path)) ;
    }

    @Test
    public void zNodeCreateAndCheckExists() throws Exception {
        String path = "/test" ;
        zkU.deleteZNode(path) ;
        zkU.createZNode(path,"test".getBytes()) ;
        assertEquals(true, zkU.zNodeExists(path));
        zkU.deleteZNode(path) ;
    }

    @Test
    public void getZNodeData() throws Exception {
        String path = "/test" ;
        zkU.deleteZNode(path) ;
        zkU.createZNode(path,"test".getBytes()) ;
        Assert.assertEquals("test",new String(zkU.getZNodeData(path)));
        zkU.deleteZNode(path) ;
    }

    @Test
    public void updateZNode() throws Exception {
        String path = "/test" ;
        zkU.deleteZNode(path) ;
        zkU.createZNode(path,"test".getBytes()) ;
        zkU.updateZNode(path, "toto".getBytes()) ;
        Assert.assertEquals("toto",new String(zkU.getZNodeData(path)));
        zkU.deleteZNode(path) ;
    }

}