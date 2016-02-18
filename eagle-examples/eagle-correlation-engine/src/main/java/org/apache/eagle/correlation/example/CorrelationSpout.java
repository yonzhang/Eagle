package org.apache.eagle.correlation.example;

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

import java.util.*;

/**
 * Created by yonzhang on 2/18/16.
 * Wrap KafkaSpout and manage metric metadata
 */
public class CorrelationSpout extends BaseRichSpout {
    // topic to KafkaSpoutWrapper
    private Map<String, KafkaSpoutWrapper> kafkaSpoutList = new HashMap<>();

    public CorrelationSpout(){
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("topic", "f1"));
    }

    private KafkaSpoutWrapper createSpout(Map conf, TopologyContext context, SpoutOutputCollector collector, String topic){
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        // write partition offset etc. into zkRoot+id
        // see PartitionManager.committedPath
        SpoutConfig config = new SpoutConfig(hosts, topic, "/eaglecorrelationconsumers", "testspout_" + topic);
        config.scheme = new SchemeAsMultiScheme(new MyScheme(topic));
        KafkaSpoutWrapper wrapper = new KafkaSpoutWrapper(config);
        SpoutOutputCollectorWrapper collectorWrapper = new SpoutOutputCollectorWrapper(collector);
        wrapper.open(conf, new TopologyContextWrapper(context, topic), collectorWrapper);
        return wrapper;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // first topic correlationtopic
        String topic1 = "correlationtopic";
        CreateTopicUtils.ensureTopicReady(topic1);
        KafkaSpoutWrapper wrapper1 = createSpout(conf, context, collector, topic1);
        kafkaSpoutList.put(topic1, wrapper1);

        // second topic correlationtopic2
        String topic2 = "correlationtopic2";
        CreateTopicUtils.ensureTopicReady(topic2);
        KafkaSpoutWrapper wrapper2 = createSpout(conf, context, collector, topic2);
        kafkaSpoutList.put(topic2, wrapper2);
    }

    @Override
    public void nextTuple() {
        for(KafkaSpoutWrapper wrapper : kafkaSpoutList.values()) {
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
        // decode and get topic
        KafkaMessageIdWrapper id = (KafkaMessageIdWrapper)msgId;
        System.out.println("acking message " + msgId + ", with topic " + id.topic);
        KafkaSpoutWrapper spout = kafkaSpoutList.get(id.topic);
        spout.ack(id.id);
    }

    @Override
    public void fail(Object msgId) {
        // decode and get topic
        KafkaMessageIdWrapper id = (KafkaMessageIdWrapper)msgId;
        System.out.println("failing message " + msgId + ", with topic " + id.topic);
        KafkaSpoutWrapper spout = kafkaSpoutList.get(id.topic);
        spout.ack(id.id);
    }

    @Override
    public void deactivate() {
        System.out.println("deactivate");
        for(KafkaSpoutWrapper wrapper : kafkaSpoutList.values()) {
            wrapper.deactivate();
        }
    }

    @Override
    public void close() {
        System.out.println("close");
        for (KafkaSpoutWrapper wrapper : kafkaSpoutList.values()) {
            wrapper.close();
        }
    }

    public static class MyScheme implements Scheme {
        private String topic;
        public MyScheme(String topic){
            this.topic = topic;
        }

        @Override
        public List<Object> deserialize(byte[] ser) {
            StringDecoder decoder = new StringDecoder(new kafka.utils.VerifiableProperties());
            Object log = decoder.fromBytes(ser);
            return Arrays.asList(topic, log);
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("f1");
        }
    }
}
