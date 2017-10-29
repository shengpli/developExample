package curator.javaapi.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Test;


/**
 * Created by lsp on 2017/10/29.
 */
public class PathDataEaxmple {
    private  CuratorFramework client=ClientExample.getClient();
    
    private static final String PATH="/test2";


    @After
    public  void close(){
        client.close();
    }


    /**
     * 普通查询
     */
    @Test
    public void queryPathData1(){
        try {
            byte[] bytes = client.getData().forPath(PATH);
            System.out.println(new String(bytes));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *  包含状态查询
     */
    @Test
    public void queryPathData2(){
        try {
            Stat stat = new Stat();
            client.getData().storingStatIn(stat).forPath(PATH);
            System.out.println(stat.getVersion());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     *  更新数据，如果未传入version参数，那么更新当前最新版本
     */
    @Test
    public void setPathData1(){
        try {
            // 普通更新
            client.setData().forPath(PATH,"新内容".getBytes());
            queryPathData1();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void setPathData2(){
        try {
            Stat stat = new Stat();
            client.getData().storingStatIn(stat).forPath(PATH);
            // 指定版本更新。需要最新版本
            client.setData().withVersion(stat.getVersion()).forPath(PATH,"新内容2".getBytes());
            queryPathData1();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
