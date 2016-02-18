package org.apache.eagle.correlation.example;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

import java.util.Map;

/**
 * Created by yonzhang on 2/18/16.
 */
public class KafkaSpoutWrapper extends KafkaSpout {
    public KafkaSpoutWrapper(SpoutConfig spoutConf) {
        super(spoutConf);
    }

    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        super.open(conf, context, collector);
    }
}
