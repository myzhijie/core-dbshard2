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

package com.jingsky.dbshard2.util;

import java.util.LinkedList;
import java.util.List;

public class RateLimiter {
	private int requests;
	private long duration;
	private List<Long> queue = new LinkedList<Long>();

	public RateLimiter(int requests, long duration) {
		this.requests = requests;
		this.duration = duration;
	}
	
	private void removeExpired() {
		long now = DateUtil.currentTimeMillis();
		while(queue.size() > 0) {
			Long reqTime = queue.get(0);
			long age = now - reqTime;
			if(age > duration) {
				queue.remove(0);
			}
			else {
				break;
			}
		}
	}
	
	public int getAvailable() {
		synchronized(this) {
			removeExpired();
			return requests - queue.size();
		}
	}
	
	public boolean isAvailable() {
		return getAvailable() > 0;
	}
	
	public boolean request() {
		synchronized(this) {
			removeExpired();
			if(queue.size()>=requests)
				return false;
			queue.add(DateUtil.currentTimeMillis());
			return true;
		}
	}
}
