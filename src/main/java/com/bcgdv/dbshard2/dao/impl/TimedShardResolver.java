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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import com.bcgdv.dbshard2.dao.ShardResolver;
import com.bcgdv.dbshard2.util.reflection.ReflectionUtil;
import com.bcgdv.dbshard2.util.DateUtil;

public abstract class TimedShardResolver<T> implements ShardResolver<T> {
    protected static final SimpleDateFormat sdf;
    static {
        sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        //sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    
    public abstract int getShardIdForTime(Long created);
    public abstract long getShardStartTime(int shardId);
    
    protected long startTime;
    private long endTime;
    protected Calendar startCal;

    public int getShardIdForTime(String yyyyMMddHHmmss) throws ParseException {
        return getShardIdForTime(sdf.parse(yyyyMMddHHmmss).getTime());
    }
    
    public int getShardIdForNow() {
        Long now = DateUtil.currentTimeMillis();
        return getShardIdForTime(now);
    }

    public int getNumberOfShards() {
    	if(getEndTime() == 0)
    		return getNumberOfShards((Long)null);
    	else
    		return getNumberOfShards(getEndTime());
    }

    public int getNumberOfShards(String endTime) throws ParseException {
        return getNumberOfShards(sdf.parse(endTime).getTime());
    }

    public int getNumberOfShards(Long endTime) {
        if(endTime == null || endTime == 0)
            endTime = DateUtil.currentTimeMillis();
        return getShardIdForTime(endTime) + 1;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
        //this.startCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        this.startCal = Calendar.getInstance();
        this.startCal.setTimeInMillis(startTime);
    }

    public void setStartTime(String yyyyMMddHHmmss) throws ParseException {
        setStartTime(sdf.parse(yyyyMMddHHmmss).getTime());
    }

    @Override
    public int getShardId(T obj) {
        Long created = 0l;
        try {
            created = (Long) ReflectionUtil.getFieldValue(obj, "created");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return getShardIdForTime(created);
    }
    
    public String getShardDateString(Long created) {
        int shardId = getShardIdForTime(created);
        long shardStartTime = getShardStartTime(shardId);
        return sdf.format(new Date(shardStartTime));
    }

    public String getShardDateString(String created) throws ParseException {
        return getShardDateString(sdf.parse(created).getTime());
    }
    
	public long getEndTime() {
		return endTime;
	}
	
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
}
