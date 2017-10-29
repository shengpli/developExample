package curator.javaapi.example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by lsp on 2017/10/29.
 */
public class ClientExample {
    public static CuratorFramework getClient(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(Constants.CONNECTION_STRING)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(6000)
                .connectionTimeoutMs(3000)
                //.namespace("test") 添加命名空间后，所有操作都会在该空间，比如多次创建节点，节点会累加创建
                .build();
        client.start();
        return client;
    }
}
