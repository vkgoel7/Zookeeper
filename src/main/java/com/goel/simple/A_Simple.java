package com.goel.simple;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class A_Simple {

    private static final Logger LOG = LoggerFactory.getLogger(A_Simple.class);

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        ZooKeeper zooKeeper = new ZooKeeper("localhost:2181,localhost:2182,localhost:2183", 15000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                LOG.info("**********************************************************************");
                LOG.info(String.format("Got the event for node: %s", watchedEvent.getPath()));
                LOG.info(String.format("Event Type: %s", watchedEvent.getType()));
            }
        });

        // Create
        zooKeeper.create("/node", "node data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, null);
        zooKeeper.create("/node/child", "child data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, null);

        // Read
        Stat stat = new Stat();
        var data = zooKeeper.getData("/node", true, stat);
        LOG.info(new String(data));
        LOG.info(String.format("Version: %d", stat.getVersion()));

        List<String> children = zooKeeper.getChildren("/node", true);
        children.forEach(child -> LOG.info(String.format("Child Found = %s", child)));

        // Update
        zooKeeper.setData("/node", "this is new Data".getBytes(), -1);

        // Read Data Again
        stat = new Stat();
        data = zooKeeper.getData("/node", true, stat);
        LOG.info(new String(data));
        LOG.info(String.format("Version: %d", stat.getVersion()));

        // Delete
        zooKeeper.delete("/node/child", -1);
        zooKeeper.delete("/node", -1);

        Thread.sleep(60000);
    }
}
