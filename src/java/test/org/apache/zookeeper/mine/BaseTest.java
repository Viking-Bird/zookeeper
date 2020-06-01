package org.apache.zookeeper.mine;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author pengwang
 * @date 2020/05/02
 */
public class BaseTest {

    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final String connectString = "localhost:2181";
    private static final int sessionTimeout = 60000;
    private static ZooKeeper zk = null;
    private static Stat stat = new Stat();

    @Before
    public void setUp() {
        try {
            zk = new ZooKeeper(connectString, sessionTimeout, new WatchTest());
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1() {
        DataTree dataTree;
        ZKDatabase zkDatabase;
        FileTxnLog fileTxnLog;
    }

    /**
     * 测试同步创建节点API
     */
    @Test
    public void testCreate() {
        try {
            String path1 = zk.create("/zk-test1-EPHEMERAL", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("Success create znode" + path1);

            String path2 = zk.create("/zk-test1-EPHEMERAL", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println("Success create znode" + path2);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试异步创建节点API
     */
    @Test
    public void testCreateWithAsync() {
        zk.create("/zk-test3-EPHEMERAL", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new IStringCallBack(), "123");
        zk.create("/zk-test3-EPHEMERAL", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new IStringCallBack(), "我是小猴");
        zk.create("/zk-test3-EPHEMERAL", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, new IStringCallBack(), "123");
    }

    /**
     * 测试使用同步API获取子节点列表
     */
    @Test
    public void testGetChildrenAPISync() {
        try {
            String path = "/zk_test_" + System.currentTimeMillis();

            zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create(path + "/c1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            List<String> childrenList = zk.getChildren(path, true);
            System.out.println(childrenList);

            zk.create(path + "/c2", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试使用异步API获取子节点列表
     */
    @Test
    public void testGetChildrenAPIASync() {
        try {
            String path = "/zk_test_" + System.currentTimeMillis();

            zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create(path + "/c1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            zk.getChildren(path, true, new IChildren2CallBack(), null);

            zk.create(path + "/c2", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试使用同步API获取数据
     */
    @Test
    public void testGetDataAPISync() {
        String path = "/zk_test_" + System.currentTimeMillis();
        try {
            zk.create(path, "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println(new String(zk.getData(path, true, stat)));
            System.out.println(stat.getCzxid() + "," + stat.getMzxid() + "," + stat.getVersion());
            zk.setData(path, "123".getBytes(), -1);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试使用异步API获取数据
     */
    @Test
    public void testGetDataAPIASync() {
        String path = "/zk_test_" + System.currentTimeMillis();
        try {
            zk.create(path, "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zk.getData(path, true, new IDataCallBack(), "我是余欢水");
            zk.setData(path, "123".getBytes(), -1);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试节点是否存在API，使用同步接口
     */
    @Test
    public void testExistAPISync() {
        String path = "/zk_test_" + System.currentTimeMillis();
        try {
            zk.exists(path, true);

            zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.setData(path, "123".getBytes(), -1);

            zk.create(path + "/c1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.delete(path + "/c1", -1);

            zk.delete(path, -1);

            Thread.sleep(Integer.MAX_VALUE);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        zk.close();
    }

    class WatchTest implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (Event.KeeperState.SyncConnected == event.getState()) {
                if (Event.EventType.None == event.getType() && null == event.getPath()) {
                    countDownLatch.countDown();
                } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    try {
                        System.out.println("最新的子节点信息是：" + zk.getChildren(event.getPath(), true));
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else if (event.getType() == Event.EventType.NodeCreated) {
                    System.out.println(event.getPath() + " Created");
                    try {
                        zk.exists(event.getPath(), true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else if (event.getType() == Event.EventType.NodeDeleted) {
                    System.out.println(event.getPath() + " Deleted");
                    try {
                        zk.exists(event.getPath(), true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else if (event.getType() == Event.EventType.NodeDataChanged) {
                    try {
                        System.out.println(event.getPath() + " DataChanged，最新值：" + new String(zk.getData(event.getPath(), true, stat)));
//                        System.out.println(stat.getCzxid() + "," + stat.getMzxid() + "," + stat.getVersion());
                        zk.exists(event.getPath(), true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    class IStringCallBack implements AsyncCallback.StringCallback {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            System.out.println("创建结果：" + rc + "，节点路径：" + path + "，传入的自定义参数：" + ctx + "，真实的节点名称：" + name);
        }
    }

    class IChildren2CallBack implements AsyncCallback.Children2Callback {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            System.out.println("获取子节点ZNode结果：" + rc + "，param path：" + path + "，ctx：" + ctx + "，子节点列表：" + children + "，stat：" + stat);
        }
    }

    class IDataCallBack implements AsyncCallback.DataCallback {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            System.out.println(rc + "," + path + "," + new String(data));
            System.out.println(stat.getCzxid() + "," + stat.getMzxid() + "," + stat.getVersion());
        }
    }
}

