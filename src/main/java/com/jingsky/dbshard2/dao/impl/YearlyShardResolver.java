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

package com.jingsky.dbshard2.dao.impl;

import java.util.Calendar;

import com.jingsky.dbshard2.util.DateUtil;

public class YearlyShardResolver<T> extends TimedShardResolver<T> {
	public YearlyShardResolver() {
	}
	
    public int getShardIdForTime(Long created) {
        if(created == null || created == 0)
            created = DateUtil.currentTimeMillis();
        
        Calendar cal = DateUtil.getCalendarInstance();
        cal.setTimeInMillis(created);

        int diffYear = cal.get(Calendar.YEAR) - startCal.get(Calendar.YEAR);
        
        return diffYear;
    }

    @Override
    public long getShardStartTime(int shardId) {
        Calendar cal = DateUtil.getCalendarInstance();
        cal.setTimeInMillis(startTime);
        cal.add(Calendar.YEAR, shardId);
        return cal.getTimeInMillis();
    }
}
