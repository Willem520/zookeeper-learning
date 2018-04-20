package com.weiyu.bigData.zookeeper;

import com.weiyu.bigData.zookeeper.watcher.SimpleWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZookeeperClient {

    @Test
    public void test() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zookeeper = new ZooKeeper("10.143.90.232:2181,10.143.90.233:2818,10.143.90.234:2181",5000,new SimpleWatcher());
        System.out.println("======zooKeeper状态："+zookeeper.getState());
        List<String> paths = zookeeper.getChildren("/storm",true);
        for (String path : paths) {
            System.out.println("======path is:"+path);
        }
        //创建节点
        String path1 = zookeeper.create("/weiyu","hello,zookeeper".getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
        System.out.println("success create znode: " + path1);

        while (true){
            Thread.sleep(5000);
            System.out.println("======一直循环");
        }
    }
}
