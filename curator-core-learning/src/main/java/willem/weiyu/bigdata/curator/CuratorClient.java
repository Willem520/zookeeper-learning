package willem.weiyu.bigdata.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/4/26 10:05
 */
public class CuratorClient {
    private CuratorFramework client;
    private String zkAddress = "10.26.27.81:2181";

    public CuratorClient(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,10);
        client =
                CuratorFrameworkFactory.builder().connectString(zkAddress).sessionTimeoutMs(5000).retryPolicy(retryPolicy).build();
        client.start();
    }

    public CuratorClient(String zkAddress){
        this.zkAddress = zkAddress;
        new CuratorClient();
    }

    public CuratorFramework getClient(){
        return client;
    }
}
