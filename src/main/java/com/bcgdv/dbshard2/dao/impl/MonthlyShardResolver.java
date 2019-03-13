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

import java.util.Calendar;

import com.bcgdv.dbshard2.util.DateUtil;

public class MonthlyShardResolver<T> extends TimedShardResolver<T> {

	public MonthlyShardResolver() {
	}
	
    public int getShardIdForTime(Long created) {
        if(created == null || created == 0)
            created = DateUtil.currentTimeMillis();
        
        Calendar cal = DateUtil.getCalendarInstance();
        cal.setTimeInMillis(created);

        int diffYear = cal.get(Calendar.YEAR) - startCal.get(Calendar.YEAR);
        int diffMonth = diffYear * 12 + cal.get(Calendar.MONTH) - startCal.get(Calendar.MONTH);
        
        return diffMonth;
    }

    @Override
    public long getShardStartTime(int shardId) {
        Calendar cal = DateUtil.getCalendarInstance();
        cal.setTimeInMillis(startTime);
        cal.add(Calendar.MONTH, shardId);
        return cal.getTimeInMillis();
    }
    
}
