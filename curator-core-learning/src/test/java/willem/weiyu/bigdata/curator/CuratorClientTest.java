package willem.weiyu.bigdata.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CuratorClientTest {
    public static final String ROOT_PATH = "/curator-test";
    private CuratorFramework curator = new CuratorClient().getClient();

    @Test
    public void testCreateNode() throws Exception {

        if (curator.checkExists().forPath(ROOT_PATH) != null) {
            String perNode = ROOT_PATH + "/persistent-node";
            //创建节点，默认为永久,creatingParentsIfNeeded为级联创建
            curator.create().creatingParentsIfNeeded().forPath(perNode, "willem".getBytes("UTF-8"));
            log.info("create persistent node:[{}]", perNode);

            String ephNode = ROOT_PATH + "/ephemeral-node";
            //创建临时节点
            curator.create().withMode(CreateMode.EPHEMERAL).forPath(ephNode);
            log.info("create ephemeral node:[{}]", ephNode);
        }
    }

    @Test
    public void testSetNode() throws Exception {
        String node = ROOT_PATH + "/persistent-node";
        if (curator.checkExists().forPath(node) != null){
            String value = "test-value";
            curator.setData().forPath(node, value.getBytes("UTF-8"));
            log.info("node:[{}] value set as:{}", node, value);
        }
    }

    @Test
    public void testGetNode() throws Exception {
        String node = ROOT_PATH + "/persistent-node";
        if (curator.checkExists().forPath(node) != null){
            byte[] dataByte = curator.getData().forPath(node);
            log.info("node:[{}] value is:{}", node, new String(dataByte));
        }
    }

    @Test
    public void testDeleteNode() throws Exception {
        String node = ROOT_PATH + "/persistent-node";
        if (curator.checkExists().forPath(node) != null){
            //guaranteed()用于强制保证删除一个节点
            curator.delete().guaranteed().forPath(node);
            log.info("create node:[{}]", node);
        }else{
            log.warn("node:[{}] is not existed", node);
        }
    }
}
