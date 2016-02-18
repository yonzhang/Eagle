package org.apache.eagle.correlation;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * Created on 2/17/16.
 * this example demostrate how Correlation topology works
 */
public class CorrelationExampleTopology {
    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        CorrelationSpout spout = new CorrelationSpout();
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
