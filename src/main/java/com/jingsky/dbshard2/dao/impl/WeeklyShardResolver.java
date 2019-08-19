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


import com.jingsky.util.common.DateUtil;

import java.text.ParseException;
import java.util.Calendar;

public class WeeklyShardResolver<T> extends FixedIntevalTimedShardResolver<T> {
	private int weekCount;

	public WeeklyShardResolver() {
		setInterval(1);
	}

	public void setInterval(int weekCount){
		setInterval(weekCount*7l * 24l * 3600000l);
	}

	public void setWeekCount(int weekCount){
		setInterval(weekCount);
	}

	public void setStartTime(long startTime) {
		//week start from sunday
		DateUtil dateUtil=new DateUtil(startTime);
		if(dateUtil.dayOfWeekInt()!=7){
			dateUtil=new DateUtil(dateUtil.weekdayStart());
			dateUtil=new DateUtil(dateUtil.addDays(-1));
		}else{
			dateUtil=new DateUtil(dateUtil.dayStart());
		}

		startTime=dateUtil.getDate().getTime();
		this.startTime = startTime;
		this.startCal = Calendar.getInstance();
		this.startCal.setTimeInMillis(startTime);
	}

	public void setStartTime(String yyyyMMddHHmmss) throws ParseException {
		setStartTime(sdf.parse(yyyyMMddHHmmss).getTime());
	}
}
