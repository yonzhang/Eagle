package org.apache.eagle.correlation;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import kafka.serializer.StringDecoder;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by yonzhang on 2/18/16.
 * Wrap KafkaSpout and manage metric metadata
 */
public class CorrelationSpout extends BaseRichSpout {
    private List<KafkaSpoutWrapper> kafkaSpoutList = new ArrayList<>();

    public CorrelationSpout(){
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("f1"));
    }

    private KafkaSpoutWrapper createSpout(Map conf, TopologyContext context, SpoutOutputCollector collector, String topic){
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        // write partition offset etc. into zkRoot+id
        // see PartitionManager.committedPath
        SpoutConfig config = new SpoutConfig(hosts, topic, "/eaglecorrelationconsumers", "testspout_" + topic);
        config.scheme = new SchemeAsMultiScheme(new MyScheme());
        KafkaSpoutWrapper wrapper = new KafkaSpoutWrapper(config);
        SpoutOutputCollectorWrapper collectorWrapper = new SpoutOutputCollectorWrapper(collector);
        wrapper.open(conf, new TopologyContextWrapper(context, topic), collectorWrapper);
        return wrapper;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // first topic correlationtopic
        KafkaSpoutWrapper wrapper1 = createSpout(conf, context, collector, "correlationtopic");
        kafkaSpoutList.add(wrapper1);

        // second topic correlationtopic2
        KafkaSpoutWrapper wrapper2 = createSpout(conf, context, collector, "correlationtopic2");
        kafkaSpoutList.add(wrapper2);
    }

    @Override
    public void nextTuple() {
        for(KafkaSpoutWrapper wrapper : kafkaSpoutList) {
            wrapper.nextTuple();
        }
    }

    /**
     * find the correct wrapper to do ack
     * that means msgId should be mapped to wrapper
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        for(KafkaSpoutWrapper wrapper : kafkaSpoutList) {
            wrapper.ack(msgId);
        }
    }

    @Override
    public void fail(Object msgId) {
        for(KafkaSpoutWrapper wrapper : kafkaSpoutList) {
            wrapper.fail(msgId);
        }
    }

    @Override
    public void deactivate() {
        for(KafkaSpoutWrapper wrapper : kafkaSpoutList) {
            wrapper.deactivate();
        }
    }

    @Override
    public void close() {
        for (KafkaSpoutWrapper wrapper : kafkaSpoutList) {
            wrapper.close();
        }
    }

    public static class MyScheme implements Scheme {
        @Override
        public List<Object> deserialize(byte[] ser) {
            StringDecoder decoder = new StringDecoder(new kafka.utils.VerifiableProperties());
            Object log = decoder.fromBytes(ser);
            return Arrays.asList(log);
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("f1");
        }
    }
}
