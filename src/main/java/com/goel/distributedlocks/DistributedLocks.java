package com.goel.distributedlocks;

import org.apache.zookeeper.*;


import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class DistributedLocks {

    private static final String LOCK_ROOT_NODE = "/locks";
    private static final String LOCK_CHILD_PREFIX = "lock-";

    private static ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String sessionId = UUID.randomUUID().toString();

        zooKeeper = new ZooKeeper("localhost:2181,localhost:2182,localhost:2183", 15000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                    try{
                        getLockOrWait(sessionId);
                    } catch (Exception ex){
                        throw new RuntimeException(ex);
                    }
                }
            }
        });

        if(zooKeeper.exists(LOCK_ROOT_NODE, false) == null){
            zooKeeper.create(LOCK_ROOT_NODE, LOCK_ROOT_NODE.getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, null);
        }

        zooKeeper.create(LOCK_ROOT_NODE + "/" + LOCK_CHILD_PREFIX, sessionId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        getLockOrWait(sessionId);

    }

    private static void getLockOrWait(String sessionId)
        throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(LOCK_ROOT_NODE, false);
        children.sort(String::compareTo);
        byte[] data = zooKeeper.getData(LOCK_ROOT_NODE + "/" + children.get(0), false, null);
        if(data != null && new String(data).equalsIgnoreCase(sessionId)){
            System.out.println("I acquired a lock.");
            Thread.sleep(30000);
            System.out.println("Releasing lock.");
            zooKeeper.delete(LOCK_ROOT_NODE + "/" + children.get(0), -1);
        }else{
            System.out.println("Could not acquire lock. Waiting");
            zooKeeper.getChildren(LOCK_ROOT_NODE, true);
        }

        Thread.sleep(1000000);
    }

}
