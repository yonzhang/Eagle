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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * write delta events to kafka
 * 1. topic should be a format like executor_state_${applicationId}
 * 2. delta event should be keyed by executorId and partitionId
 *
 * need consider close KafkaProducer and release I/O resource used by KafkaProducer
 */
public class DeltaEventKafkaPersister implements DeltaEventPersister<DeltaEventKey, byte[]>{
    private String topic;
    private KafkaProducer producer;

    public DeltaEventKafkaPersister(Config config, String applicationId){
        String topicBase = config.getString("eagle.executorState.topicBase");
        topic = topicBase + "_" + applicationId;
        Map configs = config.getObject("eagle.executorState.deltaEventKafkaProducerConfig");
        producer = new KafkaProducer(configs);
    }

    @Override
    public long store(DeltaEventKey key, byte[] event) throws Exception {
        ProducerRecord<DeltaEventKey, byte[]> record = new ProducerRecord<DeltaEventKey, byte[]>(topic, key, event);
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata recordMetadata = future.get();
        return recordMetadata.offset();
    }

    @Override
    public void close() throws IOException {
        this.producer.close();
    }
}
