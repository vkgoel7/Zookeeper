package com.goel.monitor;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class ServersToBeMonitored {

    private static final String MEMBERS_NODE = "/members";

    private static final Logger LOG = LoggerFactory.getLogger(ServersToBeMonitored.class);

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String id = UUID.randomUUID().toString();
        LOG.info(String.format(" --- My Id: %s", id));

        ZooKeeper zooKeeper = new ZooKeeper("localhost:2181,localhost:2182,localhost:2183", 15000, null);
        var creationResponse = zooKeeper.create(MEMBERS_NODE + "/" + id, id.getBytes(), OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL, null);
        LOG.info(creationResponse);

        Thread.sleep(10000000);

    }
}
