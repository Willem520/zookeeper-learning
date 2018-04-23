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
    private ZooKeeper zooKeeper;

    @Test
    public void testWatcher() throws IOException, KeeperException, InterruptedException {
        if (zooKeeper == null)
            start();
        List<String> paths = zooKeeper.getChildren("/brokers", true);
        for (String path : paths) {
            System.out.println("======path is:" + path);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            try {
                stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    @Test
    public void testCreateNode() throws IOException, KeeperException, InterruptedException {
        if (zooKeeper == null)
            start();

        //创建临时节点(会话结束后即删除)
        String path1 = zooKeeper.create("/weiyu_ephemeral", "hello,ephemeral node".getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("success create znode: " + path1);

        //创建永久节点
        String path2 = zooKeeper.create("/weiyu_persistent-", "hello,persistent node".getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println("success create znode: " + path2);

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            try {
                stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    @Test
    public void testDeleteNode() throws IOException, KeeperException, InterruptedException {
        if (zooKeeper == null)
            start();

        String nodePath = "/weiyu_persistent";
        if (zooKeeper.exists(nodePath,true) != null){
            System.out.println("======节点存在，可删除");
            zooKeeper.delete(nodePath,-1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            try {
                stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    @Test
    public void testModifyNode() throws IOException, KeeperException, InterruptedException {
        if (zooKeeper == null)
            start();

        String nodePath = "/weiyu_persistent-0000000049";
        if (zooKeeper.exists(nodePath,true) != null){
            zooKeeper.setData(nodePath,"hello,update value".getBytes("utf-8"),-1);
            System.out.println("======更新节点内容");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            try {
                stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    private void start() throws IOException {
        if (zooKeeper == null) {
            synchronized (ZookeeperClient.class) {
                zooKeeper = new ZooKeeper("10.143.90.232:2181,10.143.90.233:2181,10.143.90.234:2181", 5000, new SimpleWatcher());
            }
        } else {
            System.out.println("======zooKeeper已启动");
        }
    }

    private void stop() throws InterruptedException {
        if (zooKeeper != null){
            zooKeeper.close();
            System.out.println("======关闭zooKeeper");
        }
    }
}
