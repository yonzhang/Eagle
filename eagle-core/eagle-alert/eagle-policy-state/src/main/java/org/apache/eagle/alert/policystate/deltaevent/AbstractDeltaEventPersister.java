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
import org.apache.eagle.alert.policystate.snapshot.Snapshotable;
import org.apache.eagle.alert.policystate.snapshot.StateSnapshotDAO;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * common featurs for persisting delta events
 * 1. daemon thread to kick off snapshot action
 */
public abstract class AbstractDeltaEventPersister<Key, Value> implements DeltaEventPersister<Key, Value>{
    private Config config;
    private String applicationId;
    private String executorId;
    private Snapshotable snapshotable;
    private StateSnapshotDAO stateDAO;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public AbstractDeltaEventPersister(Config config, Snapshotable snapshotable, StateSnapshotDAO stateDAO){
        this.config = config;
        this.snapshotable = snapshotable;
        // start up daemon thread to periodically take snapshot
        long interval = config.getLong("eagle.executorState.snapshotIntervalMS");
        scheduler.scheduleWithFixedDelay(new Snapshoter(this), 0L, interval, TimeUnit.MILLISECONDS);
        // initialize StateSnapshotDAO
        this.stateDAO = stateDAO;
    }

    private static class Snapshoter implements Runnable{
        private AbstractDeltaEventPersister persister;
        public Snapshoter(AbstractDeltaEventPersister persister){
            this.persister = persister;
        }
        @Override
        public void run() {
            persister.takeSnapshot();
        }
    }

    private void takeSnapshot(){
        byte[] state = this.snapshotable.currentState();
        stateDAO.write(applicationId, executorId, state);
    }
}
