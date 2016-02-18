package org.apache.eagle.correlation.example;

/**
 * Created by yonzhang on 2/18/16.
 */
public class KafkaMessageIdWrapper {
    public Object id;
    public KafkaMessageIdWrapper(Object o){
        this.id = o;
    }
    public String topic;
}
