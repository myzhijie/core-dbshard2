/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bcgdv.dbshard2.dao.impl;

import com.bcgdv.dbshard2.util.DateUtil;

public class FixedIntevalTimedShardResolver<T> extends TimedShardResolver<T> {
    
    protected long interval;

	public FixedIntevalTimedShardResolver() {
	}
	
   @Override
    public int getShardIdForTime(Long created) {
       if(created == null || created ==0 )
           created = DateUtil.currentTimeMillis();
       long diff = created - startTime;
       int shardId = (int) (diff/interval);
       //Avoid before startTime
       if(shardId<0){
           shardId=0;
       }
       return shardId;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    @Override
    public long getShardStartTime(int shardId) {
        return startTime + shardId * interval;
    }
	
}
