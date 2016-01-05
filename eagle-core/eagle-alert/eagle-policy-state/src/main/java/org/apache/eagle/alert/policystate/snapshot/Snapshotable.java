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

package org.apache.eagle.alert.policystate.snapshot;

/**
 * interface for returning current state and recovering from state provided outside
 * This is to align with org.wso2.siddhi.core.util.snapshot.Snapshotable
 */
public interface Snapshotable {
    public byte[] currentState();
    public void restoreState(byte[] state);

    /**
     * unique Id to identity this Snapshotable object. When restoring state, it is used for check if the element still exists
     * @return
     */
    public String getElementId();
}