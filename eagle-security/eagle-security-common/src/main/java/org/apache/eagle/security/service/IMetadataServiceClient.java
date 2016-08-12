/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  * <p/>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p/>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.security.service;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Collection;

/**
 * service stub to get metadata from remote metadata service
 */
public interface IMetadataServiceClient extends Closeable, Serializable {
    Collection<HBaseSensitivityEntity> listHBaseSensitivities();
    OpResult addHBaseSensitivity(Collection<HBaseSensitivityEntity> h);
    Collection<HdfsSensitivityEntity> listHdfsSensitivities();
    OpResult addHdfsSensitivity(Collection<HdfsSensitivityEntity> h);
    Collection<IPZoneEntity> listIPZones();
    OpResult addIPZone(Collection<IPZoneEntity> h);
    Collection<HiveSensitivityEntity> listHiveSensitivities();
    OpResult addHiveSensitivity(Collection<HiveSensitivityEntity> h);
}
