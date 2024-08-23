package com.goel.distributedQueue.utility;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Helper {

    private static final Logger LOG = LoggerFactory.getLogger(Helper.class);

    public static void createNodeIfDoesNotExists(ZooKeeper zooKeeper, String queuePath) {

        try {
            if (zooKeeper.exists(queuePath, false) == null) {
                zooKeeper.create(queuePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

}
