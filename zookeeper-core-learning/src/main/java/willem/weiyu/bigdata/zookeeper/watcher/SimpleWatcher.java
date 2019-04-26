package willem.weiyu.bigdata.zookeeper.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class SimpleWatcher implements Watcher {

    public void process(WatchedEvent watchedEvent) {
        System.out.println("======watcher被触发");
        switch (watchedEvent.getType()) {
            case None:
                System.out.println("======None");
                break;
            case NodeCreated:
                System.out.println("======NodeCreated");
                break;
            case NodeDeleted:
                System.out.println("======NodeDeleted");
                break;
            case NodeDataChanged:
                System.out.println("======NodeDataChanged");
                break;
            case NodeChildrenChanged:
                System.out.println("======NodeChildrenChanged");
                break;
            default:
                System.out.println("======无匹配");
        }
    }
}
