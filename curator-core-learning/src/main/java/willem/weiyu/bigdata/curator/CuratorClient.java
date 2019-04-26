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
    private String zkAddress;
    private String rootPath;

    public CuratorClient() {
        this("10.26.27.81:2181","curator");
    }

    public CuratorClient(String zkAddress,String nameSpace) {
        this.zkAddress = zkAddress;
        this.rootPath = nameSpace;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        //namespace添加隔离空间，即指定一个zookeeper的根路径
        client =
                CuratorFrameworkFactory.builder().connectString(this.zkAddress).sessionTimeoutMs(5000)
                        .namespace(this.rootPath).retryPolicy(retryPolicy).build();
        client.start();
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
