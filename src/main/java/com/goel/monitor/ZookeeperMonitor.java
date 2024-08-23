package com.goel.monitor;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class ZookeeperMonitor {

    private static final String MEMBERS_NODE = "/members";

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperMonitor.class);

    private static ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        zooKeeper = new ZooKeeper("localhost:2181,localhost:2182,localhost:2183", 15000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                LOG.info("**********************************************");
                LOG.info(String.format("Got the event for node: %s", watchedEvent.getPath()));
                LOG.info(String.format("Event Type: %s", watchedEvent.getPath()));
                LOG.info("**********************************************");
                LOG.info("**********************************************");

                try {
                    startWatch();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }

            }
        });

        if (zooKeeper.exists(MEMBERS_NODE, false) == null) {
            zooKeeper.create(MEMBERS_NODE, "data".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, null);
        }

        startWatch();

        Thread.sleep(100000);
    }

    private static void startWatch() throws InterruptedException, KeeperException {
        if (zooKeeper != null) {
            List<String> children = zooKeeper.getChildren(MEMBERS_NODE, true, null);
            System.out.println(String.format("List of Childred: "));
            children.forEach(child -> System.out.println(child));
            System.out.println();
        }

    }

}
