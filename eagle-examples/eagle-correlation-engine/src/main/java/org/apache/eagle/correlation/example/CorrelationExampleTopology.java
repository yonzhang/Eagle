package org.apache.eagle.correlation.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

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
}
