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

package com.bcgdv.dbshard2.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.bcgdv.dbshard2.util.DateUtil;

public class CacheService {
	private static final Logger logger = Logger.getLogger(CacheService.class);
	private static ThreadLocal<Boolean> skipCache = new ThreadLocal<Boolean>();
	private CacheProxy cacheProxy;
    
    public CacheProxy getCacheProxy(){
    	if(cacheProxy == null) cacheProxy = new DummyCacheProxy();
    	return cacheProxy;
    }
    public void setCacheProxy(CacheProxy proxy){
    	this.cacheProxy = proxy;
    }
	
	public static void skipCache(boolean skip) {
	    skipCache.set(skip);
	}
	public boolean shouldSkipCache() {
		return skipCache.get() != null && skipCache.get();
	}
	
    public Object get(String key){
    	if(shouldSkipCache()) return null;
    	return getCacheProxy().get(key);
    }
    @SuppressWarnings("unchecked")
    public <T> T get(Class<T> cls, String key){
    	if(shouldSkipCache()) return null;
    	return (T)getCacheProxy().get(key);
    }
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(Class<T> cls, String key) {
    	if(shouldSkipCache()) return null;
    	return (List<T>)getCacheProxy().get(key);
    }

    public Map<String, Object> getBulk(Collection<String> keys) {
    	HashMap<String,Object> map = new HashMap<>();
    	if(!shouldSkipCache()){
    		map.putAll(getCacheProxy().getBulk(keys));
    	}
        return map;
    }
    @SuppressWarnings("unchecked")
    public <T> Map<String, T> getBulk(Class<T> cls, Collection<String> keys) {
    	HashMap<String, T> map = new HashMap<>();
    	if(!shouldSkipCache()){
    		map.putAll((Map<String, T>) getCacheProxy().getBulk(keys));
    	}
        return map;
    }
    @SuppressWarnings("unchecked")
    public <T> Map<String, List<T>> getBulkList(Class<T> cls, Collection<String> keys) {
    	HashMap<String, List<T>> map = new HashMap<>();
    	if(!shouldSkipCache()){
	        for(Entry<String,Object> entry : getCacheProxy().getBulk(keys).entrySet()){
	        	map.put(entry.getKey(),(List<T>) entry.getValue());
	        }
    	}
        return map;
    }
    
    public void set(String key, Object value){
        set(key, value,getDefaultExpiration());
    }
    public void set(String key, Object value, int expiration){
    	getCacheProxy().set(key, value,expiration);
    }

    public void setBulk(Map<String, Object> values) {
        setBulk(values,getDefaultExpiration());
    }
    public void setBulk(Map<String, Object> values, int expiration) {
    	getCacheProxy().setBulk(values,expiration);
    }
    
    public void delete(String key){
    	getCacheProxy().delete(key);
    }
    public void deleteBulk(Collection<String> keys) {
    	getCacheProxy().deleteBulk(keys);
    }

    public static  final int getDefaultExpiration(){
    	return (int) (DateUtil.DAY_SECOND*7);
    }
}
