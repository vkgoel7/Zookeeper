package com.goel.distributedQueue.utility;

import com.goel.distributedQueue.LockQueue;
import com.goel.distributedQueue.NoLockQueue;
import com.goel.distributedQueue.interfaces.Queue;
import org.apache.zookeeper.ZooKeeper;

public class QueueFactory {

    private ZooKeeper zooKeeper;
    private String queuePath;

    public QueueFactory(ZooKeeper zooKeeper, String queuePath) {
        this.zooKeeper = zooKeeper;
        this.queuePath = queuePath;
    }

    public  Queue getQueue(LockType lockType){
        if(LockType.No_Lock == lockType){
            return new NoLockQueue(zooKeeper, queuePath);
        }else{
            return new LockQueue(zooKeeper, queuePath);
        }
    }
}
