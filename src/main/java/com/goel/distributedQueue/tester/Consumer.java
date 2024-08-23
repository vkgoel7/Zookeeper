package com.goel.distributedQueue.tester;

import com.goel.distributedQueue.interfaces.Queue;
import com.goel.distributedQueue.utility.Constants;
import com.goel.distributedQueue.utility.Helper;
import com.goel.distributedQueue.utility.LockType;
import com.goel.distributedQueue.utility.QueueFactory;
import org.apache.zookeeper.ZooKeeper;

public class Consumer {

    public static void main(String[] args) throws Exception {
        LockType lockType = LockType.Lock;

        if (args.length > 0) {
            lockType = LockType.valueOf(args[0]);
        }

        ZooKeeper zooKeeper = new ZooKeeper(Constants.ZOOKEEPER_URL, 15000, null);

        if (lockType == LockType.Lock) {
            Helper.createNodeIfDoesNotExists(zooKeeper, Constants.LockRootNode);
            Helper.createNodeIfDoesNotExists(zooKeeper, Constants.QUEUE_PATH);
        }

        QueueFactory queueFactory = new QueueFactory(zooKeeper, Constants.QUEUE_PATH);

        Queue queue = queueFactory.getQueue(lockType);

        while (true) {
            String dequeueValue = queue.dequeue();
            System.out.println(dequeueValue);
            Thread.sleep(1000);
        }

    }

}
