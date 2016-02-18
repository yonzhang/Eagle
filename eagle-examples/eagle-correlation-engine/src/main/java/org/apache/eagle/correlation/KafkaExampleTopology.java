package org.apache.eagle.correlation;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import kafka.serializer.StringDecoder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created on 2/17/16.
 * this example demostrate how KafkaSpout works
 */
public class KafkaExampleTopology {
    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        SpoutConfig config = new SpoutConfig(hosts, "correlationtopic", "/eaglecorrelationconsumers", "testspout");
        config.scheme = new SchemeAsMultiScheme(new MyScheme());
        KafkaSpout spout = new KafkaSpout(config);
        builder.setSpout("testSpout", spout);
        BoltDeclarer declarer  = builder.setBolt("testBolt", new MyBolt());
        declarer.fieldsGrouping("testSpout", new Fields("f1"));
        StormTopology topology = builder.createTopology();
        boolean localMode = true;
        Config conf = new Config();
        if(!localMode){
            StormSubmitter.submitTopologyWithProgressBar("testTopology", conf, topology);
        }else{
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("testTopology", conf, topology);
            while(true) {
                try {
                    Utils.sleep(Integer.MAX_VALUE);
                } catch(Exception ex) {
                }
            }
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

    public static class MyBolt implements IRichBolt{
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        @Override
        public void execute(Tuple input) {
            System.out.println("tuple is coming: " + input);
        }

        @Override
        public void cleanup() {

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
}
