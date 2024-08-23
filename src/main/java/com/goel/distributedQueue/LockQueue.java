package com.goel.distributedQueue;

import com.goel.distributedQueue.interfaces.Queue;
import com.goel.distributedQueue.utility.DistributedLock;
import com.goel.distributedQueue.utility.Helper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class LockQueue implements Queue {

    private static final String ITEM_PREFIX = "/item";

    private ZooKeeper zooKeeper;
    private String queuePath;

    public LockQueue(ZooKeeper zooKeeper, String queuePath) {
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
        while(true){

            List<String> children = zooKeeper.getChildren(queuePath, false);
            if(children.isEmpty()){
                return null;
            }

            children.sort(String::compareTo);

            for(String node: children){
                boolean lockAcquired = false;
                DistributedLock distributedLock = null;

                try{
                    String nodePath = queuePath + "/" + node;
                    String item = new String(zooKeeper.getData(nodePath, false, null));

                    distributedLock = new DistributedLock(zooKeeper, queuePath);
                    lockAcquired = distributedLock.lock();

                    if(lockAcquired && zooKeeper.exists(nodePath, false) != null){
                        zooKeeper.delete(nodePath, -1);
                        return item;
                    }

                }catch (Exception ex){
                    throw new RuntimeException(ex);
                }finally {
                    distributedLock.unlock();
                }

            }

        }
    }
}
