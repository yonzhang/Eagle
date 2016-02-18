package org.apache.eagle.correlation.example;

import kafka.admin.AdminUtils;
import org.I0Itec.zkclient.ZkClient;

import java.util.Properties;

/**
 * Created by yonzhang on 2/18/16.
 */
public class CreateTopicUtils {
    public static void ensureTopicReady(String topic){
        ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000);
        if(!AdminUtils.topicExists(zkClient, topic)) {
            AdminUtils.createTopic(zkClient, topic, 2, 1, new Properties());
        }
    }
}
