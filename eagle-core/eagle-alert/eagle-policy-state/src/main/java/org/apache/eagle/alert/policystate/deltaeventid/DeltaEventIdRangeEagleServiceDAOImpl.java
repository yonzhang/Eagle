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

package org.apache.eagle.alert.policystate.deltaeventid;

import com.typesafe.config.Config;
import org.apache.eagle.alert.policystate.entity.DeltaEventIdRangeEntity;
import org.apache.eagle.alert.policystate.ExecutorStateConstants;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * DAO methods backed by eagle service
 */
public class DeltaEventIdRangeEagleServiceDAOImpl implements DeltaEventIdRangeDAO {
    private final static Logger LOG = LoggerFactory.getLogger(DeltaEventIdRangeEagleServiceDAOImpl.class);
    private Config config;

    public DeltaEventIdRangeEagleServiceDAOImpl(Config config){
        this.config = config;
    }

    @Override
    public void write(final String site, final String applicationId, final String executorId, long id) throws IOException{
        LOG.info("write offset: " + applicationId + "/" + executorId + ", with offset " + id);
        DeltaEventIdRangeEntity entity = new DeltaEventIdRangeEntity();
        entity.setTags(new HashMap<String, String>(){{
            put("site", site);
            put("applicationId", applicationId);
            put("executorId", executorId);
        }});
        entity.setStartingOffset(id);
        IEagleServiceClient client = new EagleServiceClientImpl(new EagleServiceConnector(config));
        GenericServiceAPIResponseEntity response = null;
        try{
            response = client.create(Arrays.asList(entity));
        }catch(Exception ex){
            LOG.error("fail creating entity", ex);
            throw new IOException(ex);
        }
        if(!response.isSuccess()){
            LOG.error("fail creating entity with exception " + response.getException());
            throw new IOException(response.getException());
        }
        client.close();
        LOG.info("end write offset: " + applicationId + "/" + executorId);
    }

    @Override
    public long findLatestId(String site, String applicationId, String executorId) throws IOException {
        IEagleServiceClient client = new EagleServiceClientImpl(new EagleServiceConnector(config));
        String query = ExecutorStateConstants.POLICY_STATE_DELTA_EVENT_ID_RANGE_SERVICE_ENDPOINT_NAME + "[@applicationId=\"" + applicationId +
                "\" AND @site=\"" + site +
                "\" AND @executorId=\"" + executorId +
                "\"]{*}";
        GenericServiceAPIResponseEntity<DeltaEventIdRangeEntity> response = null;
        try {
            response = client.search()
                    .startTime(0)
                    .endTime(new Date().getTime())
                    .pageSize(1)
                    .query(query)
                    .send();
        }catch(Exception ex){
            LOG.error("error querying delta event Id range" + ex);
            throw new IOException(ex);
        }
        client.close();
        if (response.getException() != null) {
            throw new IOException("Got an exception when query eagle service: " + response.getException());
        }
        List<DeltaEventIdRangeEntity> entities = response.getObj();
        if(entities.size() >= 1){
            return entities.get(0).getStartingOffset();
        }
        return Long.MAX_VALUE; // in this case, recovering will be ignored
    }
}
