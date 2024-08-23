package com.goel.distributedQueue.tester;

import com.goel.distributedQueue.NoLockQueue;
import com.goel.distributedQueue.interfaces.Queue;
import com.goel.distributedQueue.utility.Constants;
import org.apache.zookeeper.ZooKeeper;

public class Producer {

    public static void main(String[] args) throws Exception {

        ZooKeeper zooKeeper = new ZooKeeper(Constants.ZOOKEEPER_URL, 15000, null);

        Queue queue = new NoLockQueue(zooKeeper, Constants.QUEUE_PATH);

        for (int index = 0; index <= 10000; index++) {
            System.out.println("Going to produce item" + index);
            queue.enqueue("item" + index);
            Thread.sleep(1000);
        }

        zooKeeper.close();
    }

}
