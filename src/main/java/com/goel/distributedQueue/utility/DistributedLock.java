package com.goel.distributedQueue.utility;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class DistributedLock {

    private ZooKeeper zooKeeper;
    private String nodePath;
    private String lockNode;

    public DistributedLock(ZooKeeper zooKeeper, String path) {
        this.zooKeeper = zooKeeper;
        this.nodePath = Constants.LockRootNode + path;
        Helper.createNodeIfDoesNotExists(zooKeeper, Constants.LockRootNode);
        Helper.createNodeIfDoesNotExists(zooKeeper, this.nodePath);
    }

    public boolean lock() throws InterruptedException, KeeperException {
        lockNode = zooKeeper.create(nodePath + "/lock-", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        List<String> children = zooKeeper.getChildren(nodePath, false);
        children.sort(String::compareTo);

        int indexOfLockNodeCreate = children.indexOf(lockNode.substring(lockNode.lastIndexOf('/') + 1));
        if(indexOfLockNodeCreate == 0){
            return true;
        }else{
            zooKeeper.delete(lockNode, -1 );
            return false;
        }
    }

    public void unlock() throws InterruptedException, KeeperException {
        zooKeeper.delete(lockNode, -1);
    }

}
