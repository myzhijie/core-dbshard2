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

abstract public class CacheProxyBase implements CacheProxy {
    private static final Logger logger = Logger.getLogger(CacheProxyBase.class);
	
	@Override
	public Object get(String key) {
        Object value = null;
		try {
		    value = get(key, Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}

    @Override
	public Object getAndTouch(String key, int expiration) {
        Object value = null;
		try {
		    value = get(key, expiration);
		} catch (Exception e) {
			e.printStackTrace();
		}
        return value;
	}
    
    @Override
    public Map<String, Object> getBulk(Collection<String> keys) {
        HashMap<String, Object> values = new HashMap<String, Object>();
        for(String key : keys){
            try {
                values.put(key, get(key, Integer.MAX_VALUE));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return values;
    }

    @Override
    public void set(String key, Object value){
        set(key,value, Integer.MAX_VALUE);
    }

    @Override
    public void setBulk(Map<String, Object> entities){
        setBulk(entities,Integer.MAX_VALUE);
    }
    
    @Override
    public void setBulk(Map<String, Object> entities, int expiration){
        for(Entry<String,Object> entry : entities.entrySet()){
            set(entry.getKey(),entry.getValue(), expiration);
        }
    }

    @Override
    public void deleteBulk(Collection<String> keys){
        for(String key : keys){
            delete(key);
        }
    }
	
	public long incr(String key, long delta) {
		long ret = 0;
		try {
			ret = incr(key, delta, Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	public long incrAndTouch(String key, long delta, int expiration) {
		long ret = 0;
		try {
			ret = incr(key, delta, expiration);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}
	
	public long decr(String key, long delta) {
		long ret = 0;
		try {
			ret = decr(key, delta, Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	public long decrAndTouch(String key, long delta, int expiration) {
		long ret = 0;
		try {
			ret = decr(key, delta, expiration);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}
    
    public static <T> List<List<T>> splitList(List<T> items, int size){
        List<List<T>> list = new ArrayList<List<T>>();
        int n = items.size();
        if(n == 0){
        }else if(n<=size){
            list.add(items);
        }else{
            list.addAll(splitList(items.subList(0, n/2),size));
            list.addAll(splitList(items.subList(n/2, n),size));
        }
        return list;
    }
    
	@Override
	public <T> T getObject(String key) {
		return (T)get(key);
	}
	@Override
	public <T> T getObject(String key, int expiration) {
		return (T) get(key, expiration);
	}
	@Override
	public <T> T getAndTouchObject(String key, int expiration) {
		return (T) getAndTouch(key, expiration);
	}
	@Override
	public <T> Map<String, T> getBulkObjects(Collection<String> keys) {
		return (Map<String, T>) getBulk(keys);
	}
}
