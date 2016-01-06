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

package org.apache.eagle.alert.policystate;

import com.typesafe.config.Config;
import org.apache.eagle.alert.policystate.deltaevent.DeltaEventDAO;
import org.apache.eagle.alert.policystate.deltaevent.DeltaEventKafkaDAOImpl;
import org.apache.eagle.alert.policystate.deltaevent.DeltaEventReplayCallback;
import org.apache.eagle.alert.policystate.deltaeventid.DeltaEventIdRangeDAO;
import org.apache.eagle.alert.policystate.deltaeventid.DeltaEventIdRangeEagleServiceDAOImpl;
import org.apache.eagle.alert.policystate.snapshot.Snapshotable;
import org.apache.eagle.alert.policystate.snapshot.StateSnapshotEagleServiceDAOImpl;
import org.apache.eagle.alert.policystate.snapshot.StateSnapshotService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * manage state lifecycle
 * 1. initialization
 * 2. periodically take snapshot
 * 3. persist delta events
 * 4. restore state including restore snapshot and replay delta events
 */
public class StateMgmtService{
    private static final Logger LOG = LoggerFactory.getLogger(StateMgmtService.class);

    private Config config;
    private transient StateSnapshotService _snapshotService;
    private Object _snapshotLock;
    private DeltaEventDAO _deltaEventDAO;
    private DeltaEventIdRangeDAO _deltaEventIdRangeDAO;
    private AtomicBoolean _shouldPersistIdRange = new AtomicBoolean(false);
    private Snapshotable snapshotable;

    public StateMgmtService(Config config, final EventReplayable replayable, Object snapshotLock, Snapshotable snapshotable){
        this.config = config;
        _snapshotLock = snapshotLock;
        _deltaEventDAO = new DeltaEventKafkaDAOImpl(config, snapshotable.getElementId());
        _deltaEventIdRangeDAO = new DeltaEventIdRangeEagleServiceDAOImpl(config, snapshotable.getElementId());
        // recover state from remote storage. state recovery only happens when this bolt is started
        StateRecoveryService recoverySvc = new StateRecoveryService(config,
                snapshotable,
                new StateSnapshotEagleServiceDAOImpl(config),
                _deltaEventDAO,
                _deltaEventIdRangeDAO,
                new DeltaEventReplayCallback() {
                    @Override
                    public void replay(Object event) {
                        try {
                            // invoke worker to replay message
                            replayable.replay(event);
                        }catch(Exception ex){
                            LOG.error("failing replay event " + event);
                        }
                    }
                }
        );
        recoverySvc.recover();
        // make sure snapshot only works after recover is done
        _snapshotService =
                new StateSnapshotService(config, snapshotable, new StateSnapshotEagleServiceDAOImpl(config), _snapshotLock, _shouldPersistIdRange);
    }

    public void dispatchToDeltaEventPersist(List input) throws Exception{
        long offset = _deltaEventDAO.write(input);
        if(_shouldPersistIdRange.get()){
            _deltaEventIdRangeDAO.write(offset);
            // flip this flat
            _shouldPersistIdRange.set(false);
        }
    }
}
