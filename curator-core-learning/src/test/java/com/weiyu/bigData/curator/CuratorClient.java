package com.weiyu.bigData.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class CuratorClient {
    private CuratorFramework curator;
    @Test
    public void testCreateNode() throws Exception {
        if(curator == null)
            start();

        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/curator_persistent","hello,curator".getBytes("utf-8"));
    }

    private void start(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,10);
        curator = CuratorFrameworkFactory.builder().connectString("10.143.90.232:2181,10.143.90.233:2181,10.143.90.234:2181").sessionTimeoutMs(5000).retryPolicy(retryPolicy).build();
        curator.start();
    }

    private void stop(){
        if (curator != null)
            curator.close();
    }
}
