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

package com.jingsky.dbshard2.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DummyCacheProxy implements CacheProxy {
	private static final DummyCacheProxy instance = new DummyCacheProxy();
	public static CacheProxy getInstance() {
		return instance;
	}
	
	private Map<String, Object> emptyMap = new HashMap<String, Object>();

	@Override
	public Object get(String key) {
		return null;
	}

	@Override
	public Object get(String key, int expiration) {
		return null;
	}

	@Override
	public Object getAndTouch(String key, int expiration) {
		return null;
	}

	@Override
	public Map<String, Object> getBulk(Collection<String> keys) {
		return emptyMap;
	}

	@Override
	public void set(String key, Object value) {
	}

	@Override
	public void set(String key, Object value, int expiration) {
	}

	@Override
	public void setBulk(Map<String, Object> entities) {
	}

	@Override
	public void setBulk(Map<String, Object> entities, int expiration) {
	}

	@Override
	public void delete(String key) {
	}

	@Override
	public void deleteBulk(Collection<String> keys) {
	}

	@Override
	public long incr(String key, long delta, int expiration) {
		return 0;
	}

	@Override
	public long decr(String key, long delta, int expiration) {
		return 0;
	}

	@Override
	public CacheType getType() {
		return CacheType.nocache;
	}

	@Override
	public <T> T getObject(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getObject(String key, int expiration) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getAndTouchObject(String key, int expiration) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Map<String, T> getBulkObjects(Collection<String> keys) {
		// TODO Auto-generated method stub
		return null;
	}
}
