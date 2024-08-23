package com.goel.distributedQueue;

import com.goel.distributedQueue.interfaces.Queue;
import com.goel.distributedQueue.utility.Helper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.Arrays;
import java.util.List;

public class NoLockQueue implements Queue {

    private static final String ITEM_PREFIX = "/item";

    private ZooKeeper zooKeeper;
    private String queuePath;

    public NoLockQueue(ZooKeeper zooKeeper, String queuePath) {
        this.zooKeeper = zooKeeper;
        this.queuePath = queuePath;
        Helper.createNodeIfDoesNotExists(zooKeeper, queuePath);
    }

    @Override
    public void enqueue(String itemData) throws Exception {
        zooKeeper.create(queuePath+ITEM_PREFIX, itemData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, null);
    }

    @Override
    public String dequeue() throws Exception {

        List<String> children = zooKeeper.getChildren(queuePath, false);
        if(children.isEmpty()){
            return null;
        }

        children.sort(String::compareTo);

        String nodePath = queuePath + "/" + children.get(0);
        String data = new String(zooKeeper.getData(nodePath, false, null));
        zooKeeper.delete(nodePath, -1);

        return data;
    }
}
