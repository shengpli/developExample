package curator.javaapi.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Test;

/**
 * Created by lsp on 2017/10/29.
 * 各cache的区别：
 * nodeCache：监听当前节点的创建、删除、修改数据事件
 * pathCache：监听一级子节点的创建、删除、修改数据事件
 * treeCache：监听当前结点以及所有子节点的创建、删除、修改数据事件
 */
public class CacheExample {
    private CuratorFramework client = ClientExample.getClient();

    @After
    public void close() {
        client.close();
    }

    /**
     * NodeCache监听当前节点的创建、删除、数据修改，当前节点不需要事先存在
     * 经过试验，发现注册监听之后，如果先后多次修改监听节点的内容，部分监听事件会发生丢失现象。
     * 原因：每次触发事件后，watcher重新注册需要时间，在注册时间内无法收到事件。可在事件前后添加Thread.sleep(2000)测试
     *
     * @throws Exception
     */
    @Test
    public void nodeCache1() throws Exception {
        String path = "/test";
        NodeCache nodeCache = new NodeCache(client, path);
        nodeCache.start();
        //对节点的监听需要配合回调函数来进行处理接收到监听事件之后的业务处理。NodeCache通过NodeCacheListener来完成后续处理
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("监听事件触发");
                System.out.println("重新获得节点内容为：" + new String(nodeCache.getCurrentData().getData()));
            }
        });
        client.create().forPath(path);
        Thread.sleep(2000);
        client.delete().forPath(path);
        Thread.sleep(2000);
        client.setData().forPath(path, "456".getBytes());
        client.setData().forPath(path, "789".getBytes());
        client.setData().forPath(path, "123".getBytes());
        client.setData().forPath(path, "222".getBytes());
        client.setData().forPath(path, "333".getBytes());
        client.setData().forPath(path, "444".getBytes());

        Thread.sleep(5000);

    }

    /**
     * NodeCache还可以监听创建子节点事件，无论创建成功还是失败，无法监听子节点修改和删除事件
     *
     * @throws Exception
     */
    @Test
    public void nodeCache2() throws Exception {
        String path = "/test";
        NodeCache nodeCache = new NodeCache(client, path);
        nodeCache.start();
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("监听事件触发");
                System.out.println("重新获得节点内容为：" + new String(nodeCache.getCurrentData().getData()));
            }
        });

        System.out.println("创建节点");
        client.create().forPath(path + "/test", "123".getBytes());
        Thread.sleep(5000);
        System.out.println("修改数据");
        client.setData().forPath(path + "/test", "456".getBytes());
        Thread.sleep(5000);
        System.out.println("删除节点");
        client.delete().forPath(path + "/test");
        Thread.sleep(5000);

    }


    /**
     * pathChildrenCache监听一级子节点的创建、删除、数据修改，子节点不需要事先存在;二级子节点，创建事件能触发监听，删除事件无法触发监听
     *
     * @throws Exception
     */
    @Test
    public void pathChildrenCache1() throws Exception {
        String parentPath = "/p1";
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, parentPath, true);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                System.out.println("事件类型：" + event.getType() + "；操作节点：" + event.getData().getPath()
                        + "；节点数据：" + event.getData());
            }
        });

        String path = "/p1/c1";
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, "123".getBytes());
        Thread.sleep(2000); // 如果没线程睡眠则无法触发监听事件
        client.setData().forPath(path, "345".getBytes());
        Thread.sleep(2000);
        client.delete().forPath(path);
        Thread.sleep(2000);
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
        Thread.sleep(2000);
        client.delete().forPath(path);
        Thread.sleep(2000);

        //二级子节点，创建事件能触发监听，删除事件无法触发监听
        String path2 = "/p1/c1/d1";
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path2);
        Thread.sleep(2000);
        client.delete().forPath(path2);

        Thread.sleep(5000);
    }

    /**
     * treeCache监听当前结点以及所有子节点的创建、删除、修改节点数据事件
     *
     * @throws Exception
     */
    @Test
    public void treeCache() throws Exception {
        String parentPath = "/flume";
        TreeCache treeCache = new TreeCache(client, parentPath);
        treeCache.start();
        treeCache.getListenable().addListener((client, event) -> {
            System.out.println("事件类型：" + event.getType() + "；操作节点：" + event.getData().getPath()
                    + "；节点数据：" + new String(event.getData().getData()));
        });


        //当前结点，创建、修改数据、删除
        client.create().withMode(CreateMode.PERSISTENT).forPath(parentPath, "创建当前结点".getBytes());
        Thread.sleep(2000);
        client.setData().forPath(parentPath, "修改当前结点数据".getBytes());
        Thread.sleep(2000);
        client.delete().forPath(parentPath);
        Thread.sleep(2000);

        //一级子节点，创建、修改数据、删除
        String path = "/p2/c2";
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, "创建一级子结点".getBytes());
        Thread.sleep(2000);
        client.setData().forPath(path, "修改一级子结点数据".getBytes());
        Thread.sleep(2000);
        client.delete().deletingChildrenIfNeeded().forPath(path);
        Thread.sleep(2000);

        //二级子节点，创建、修改数据、删除
        String path2 = "/p2/c2/d2";
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path2, "创建二级子结点".getBytes());
        Thread.sleep(2000);
        client.setData().forPath(path2, "修改二级子结点数据".getBytes());
        Thread.sleep(2000);
        client.delete().deletingChildrenIfNeeded().forPath(path2);
        Thread.sleep(5000);
    }


}
