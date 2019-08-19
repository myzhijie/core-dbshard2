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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.BulkFuture;

import org.apache.log4j.Logger;

public class SpyMemcachedProxy extends CacheProxyBase {
    private static final Logger logger = Logger.getLogger(SpyMemcachedProxy.class);
    protected static final int KEYS_PER_BULK = 40;
	protected String protocol;
	protected MemcachedClient spyMemcachedClient;
	
	public String getProtocol() {
		return this.protocol;
	}
	public void setProtocol(String protocol) {
		logger.info("set protocol "+protocol);
		this.protocol = protocol;
	}
	public MemcachedClient getSpyMemcachedClient(){
		return this.spyMemcachedClient;
	}
	public void setSpyMemcachedClient(MemcachedClient spyMemcachedClient) {
		this.spyMemcachedClient = spyMemcachedClient;
	}
    @Override
    public CacheType getType() {
        return CacheType.spymemcached;
    }
	
	@Override
	public Object get(String key, int expiration) {
	    logger.info("get start for key: " + key+" expiration: "+expiration);
		Object value = null;
		if (key != null) {
			if (expiration > -1) {
				if (getProtocol().equalsIgnoreCase("binary")) {
				    try{
    					CASValue<Object> obj = spyMemcachedClient.asyncGetAndTouch(key, expiration).get(2,TimeUnit.SECONDS);
    					if (obj != null) {
    						value = obj.getValue();
    					}
				    }catch(Exception e){
				        e.printStackTrace();
				    }
				} else {
					value = spyMemcachedClient.get(key);
				}
			} else {
				value = spyMemcachedClient.get(key);
			}
		}
	    logger.info("get end for key " + key);
		return value;
	}

    @Override
	public Map<String, Object> getBulk(Collection<String> keys) {
	    logger.info("getBulk start for keys " + keys.hashCode() + ". size: " + keys.size());
		Map<String, Object> cache = new HashMap<String, Object>();
        List<List<String>> spKeys = splitList(new ArrayList<String>(keys), KEYS_PER_BULK);
        List<BulkFuture<Map<String, Object>>> futures = new ArrayList<BulkFuture<Map<String, Object>>>();
        for(List<String> list : spKeys){
            futures.add(spyMemcachedClient.asyncGetBulk(list));
        }
        for(BulkFuture<Map<String, Object>> fut : futures){
    		try{
	            Map<String, Object> cacheTemp = fut.getSome(2, TimeUnit.SECONDS);
	            if(cacheTemp != null)
	                cache.putAll(cacheTemp);
    		}catch(Exception e){
    		    e.printStackTrace();
    		}
        }
        logger.info("getBulk end for keys " + keys.hashCode());
		return cache;
	}

    @Override
	public void set(String key, Object value, int expiration) {
	    logger.info("set start for key: " + key+" expiration: "+expiration);
		spyMemcachedClient.set(key, expiration, value);
	    logger.info("set end for key " + key);
	}

	@Override
	public void delete(String key) {
	    logger.info("delete start for key: " + key);
		if (key != null) {
			spyMemcachedClient.delete(key);
		}
	    logger.info("delete end for key: " + key);
	}

	@Override
	public long incr(String key, long delta, int expiration) {
	    logger.info("incr start for key: " + key+" delta: "+delta+" expiration: "+expiration);
		long value = 0;
		if (key != null) {
		    try{
    			if (expiration > -1) {
    				value = spyMemcachedClient.asyncIncr(key, delta, 0, expiration).get(1, TimeUnit.SECONDS);
    			} else {
    				value = spyMemcachedClient.asyncIncr(key, delta, 0).get(1, TimeUnit.SECONDS);
    			}
		    }catch(Exception e){
		        e.printStackTrace();
		    }
		}
		logger.info("incr end for key: " + key);
		return value;
	}
	
	@Override
	public long decr(String key, long delta, int expiration) {
	    logger.info("decr start for key: " + key+" delta: "+delta+" expiration: "+expiration);
		long value = 0;
		if (key != null) {
            try{
    			if (expiration > -1) {
    				value = spyMemcachedClient.asyncDecr(key, delta, 0, expiration).get(1, TimeUnit.SECONDS);
    			}
    			else {
    				value = spyMemcachedClient.asyncDecr(key, delta, 0).get(1, TimeUnit.SECONDS);
    			}
            }catch(Exception e){
                e.printStackTrace();
            }
		}
		logger.info("decr end for key: " + key);
		return value;
	}
}
