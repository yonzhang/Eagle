/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.policystate.deltaevent;

import com.typesafe.config.Config;
import org.apache.eagle.alert.policystate.ExecutorStateConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * writeState delta events to kafka
 * 1. topic should be a format like executor_state_${applicationId}
 * 2. delta event should be keyed by executorId and partitionId
 *
 * need consider close KafkaProducer and release I/O resource used by KafkaProducer
 */
public class DeltaEventKafkaDAOImpl implements DeltaEventDAO {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaEventKafkaDAOImpl.class);
    private String topic;
    private KafkaProducer producer;
    private KafkaConsumer consumer;
    private int partitionNum;
    private String site;
    private String applicationId;
    private String elementId;

    public DeltaEventKafkaDAOImpl(Config config, String elementId){
        // create KafkaProducer
        this.elementId = elementId;
        this.site = config.getString("eagleProps.site");
        this.applicationId = config.getString("eagleProps.dataSource");
        String topicBase = config.getString("eagleProps.executorState.topicBase");
        topic = topicBase + "_" + site + "_" + applicationId;
        Map producerConfigs = config.getObject("eagleProps.executorState.deltaEventKafkaProducerConfig").unwrapped();
        producer = new KafkaProducer(producerConfigs);

        // create KafkaConsumer
        Map consumerConfigs = config.getObject("eagleProps.executorState.deltaEventKafkaConsumerConfig").unwrapped();
        consumer = new KafkaConsumer(consumerConfigs);

        // fetch number of partitions
        String zkPath = ExecutorStateConstants.ZOOKEEPER_ZKPATH_DEFAULT;
        try{
            zkPath = config.getString(ExecutorStateConstants.ZOOKEEPER_ZKPATH_PROPERTY);
        }catch(Exception ex){
            // do nothing
            LOG.warn(ExecutorStateConstants.ZOOKEEPER_ZKPATH_PROPERTY + "is not set");
        }
        String zkConnection = config.getString("eagleProps.executorState.zkClientConnection");
        KafkaTopicInfoReader reader = new KafkaTopicInfoReader(config, zkConnection, zkPath, topic);
        int numPartitions = reader.getNumPartitions();
        reader.close();
        DeltaEventKey key = new DeltaEventKey();
        key.setSite(site);
        key.setElementId(elementId);
        key.setApplicationId(applicationId);
        partitionNum = Math.abs(key.hashCode()) % numPartitions;
    }

    @Override
    public long write(Object event) throws Exception {
        DeltaEventKey key = new DeltaEventKey();
        key.setSite(site);
        key.setElementId(elementId);
        key.setApplicationId(applicationId);
        DeltaEventValue value = new DeltaEventValue();
        value.setElementId(elementId);
        value.setEvent(event);
        ProducerRecord<DeltaEventKey, Object> record = new ProducerRecord<DeltaEventKey, Object>(topic, partitionNum, key, value);
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata recordMetadata = future.get();
        return recordMetadata.offset();
    }

    private long findCurrentMaxOffset(){
        TopicPartition partition = new TopicPartition(topic, partitionNum);
        Map<TopicPartition, Long> tps = consumer.offsetsBeforeTime(-2, Arrays.asList(partition));
        return tps.get(partition);
    }

    @Override
    public void load(long startOffset, DeltaEventReplayCallback callback) throws Exception{
        long maxOffset = findCurrentMaxOffset();
        boolean isRunning = true;
        while(isRunning) {
            Map<String, ConsumerRecords> records = consumer.poll(100);
            List<ConsumerRecord> ret = records.get(topic).records();
            long offset = ret.get(ret.size()-1).offset();
            if(offset >= maxOffset)
                isRunning = false;
            else
                isRunning = true;
        }
    }

    @Override
    public void close() throws IOException {
        this.producer.close();
    }
}
