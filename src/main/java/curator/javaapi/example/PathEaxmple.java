package curator.javaapi.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Test;


/**
 * Created by lsp on 2017/10/29.
 */
public class PathEaxmple {
    private  CuratorFramework client=ClientExample.getClient();

    @After
    public  void close(){
        client.close();
    }


    /**
     * 创建一个初始内容为空的节点
     */
    @Test
    public void createNullDataPath(){
        try {
            client.create().forPath("/test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一个包含内容的节点
     */
    @Test
    public void createContainsDataPath(){
        try {
            client.create().forPath("/test2","test data".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建临时节点，并递归创建父节点.
     * 在递归创建父节点时，父节点为持久节点。
     * 客户端关闭，临时节点就会消失
     */
    @Test
    public void createEphemeralPath(){
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/test3/test");
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除一个节点,结点下不能包含子节点
     */
    @Test
    public void deletePath(){
        try {
            client.delete().forPath("/test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除节点并递归删除其子节点
     */
    @Test
    public void deletePath2(){
        try {
            client.delete().deletingChildrenIfNeeded().forPath("/test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 指定版本进行删除.默认版本为0
     * 如果此版本已经不存在，则删除异常，异常信息如下。
     * org.apache.zookeeper.KeeperException$BadVersionException: KeeperErrorCode = BadVersion for
     */
    @Test
    public void deletePath3(){
        try {
            client.delete().withVersion(0).forPath("/test3");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 强制保证删除一个节点
     * 只要客户端会话有效，那么Curator会在后台持续进行删除操作，直到节点删除成功。比如遇到一些网络异常的情况，此guaranteed的强制删除就会很有效果。
     */
    @Test
    public void deletePath4(){
        try {
            client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/test2");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
