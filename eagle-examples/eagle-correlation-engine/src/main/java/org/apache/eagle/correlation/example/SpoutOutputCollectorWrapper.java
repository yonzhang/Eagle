package org.apache.eagle.correlation.example;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;

import java.util.List;

/**
 * Created by yonzhang on 2/18/16.
 * wrap SpoutOutputCollector so Eagle can intercept the message sent from within KafkaSpout
 *    collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset));
 */
public class SpoutOutputCollectorWrapper extends SpoutOutputCollector {
    private ISpoutOutputCollector delegate;
    public SpoutOutputCollectorWrapper(ISpoutOutputCollector delegate){
        super(delegate);
        this.delegate = delegate;
    }
    @Override
    public List<Integer> emit(List<Object> tuple, Object messageId) {
        // decode tuple to have field topic and field f1
        String topic = (String)tuple.get(0);
        System.out.println("emitted tuple: " + tuple + ", with message Id: " + messageId + ", with topic " + topic);
        KafkaMessageIdWrapper newMessageId = new KafkaMessageIdWrapper(messageId);
        newMessageId.topic = topic;
        delegate.emit("default", tuple, newMessageId);
        return null;
    }
}
