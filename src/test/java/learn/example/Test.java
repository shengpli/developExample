package learn.example;

import curator.javaapi.example.ClientExample;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.CreateMode;
import org.junit.After;

public class Test {

    private CuratorFramework client = ClientExample.getClient();

    @After
    public void close() {
        client.close();
    }

    @org.junit.Test
    public void test() throws Exception {
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/flume/10.255.10.16/cgip_roxy/sto-ut", "创建临时结点".getBytes());
        Thread.sleep(5000);
    }

    @org.junit.Test
    public void treeCache() throws Exception {
        String parentPath = "/flume";
        TreeCache treeCache = new TreeCache(client, parentPath);
        treeCache.start();
        treeCache.getListenable().addListener((client, event) -> {
            System.out.println("事件类型：" + event.getType() + "；操作节点：" + event.getData().getPath()
                    + "；节点数据：" + new String(event.getData().getData()));
        });


        int a = 0;
        while (true) {
            Thread.sleep(1000);
        }
    }
}
