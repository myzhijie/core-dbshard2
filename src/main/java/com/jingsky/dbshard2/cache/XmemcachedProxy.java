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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClient;
import net.rubyeye.xmemcached.utils.Protocol;

import org.apache.log4j.Logger;

public class XmemcachedProxy extends CacheProxyBase {
	private static final Logger logger = Logger.getLogger(XmemcachedProxy.class);
	protected MemcachedClient xMemcachedClient;
    
	public MemcachedClient getXMemcachedClient(){
		return this.xMemcachedClient;
	}
	public void setXMemcachedClient(MemcachedClient xMemcachedClient) {
		this.xMemcachedClient = xMemcachedClient;
	}

    @Override
    public CacheType getType() {
        return CacheType.xmemcached;
    }
    
	@Override
	public Object get(String key, int expiration) {
	    logger.info("get start for key: " + key+" expiration: "+expiration);
		Object value = null;
		if (key != null) {
			try {
				if (expiration > -1) {
					if (xMemcachedClient.getProtocol().equals(Protocol.Text)) {
						value = xMemcachedClient.get(key);
						if (value != null) {
							xMemcachedClient.touch(key, expiration);
						}
					} else {
						value = xMemcachedClient.getAndTouch(key,expiration);
					}
				} else {
					value = xMemcachedClient.get(key);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	    logger.info("get end for key " + key + (value == null ? ", not found" : ", found") );
        return value;
	}

    @Override
	public Map<String, Object> getBulk(Collection<String> keys) {
	    logger.info("getBulk start for keys " + keys.hashCode() + ". size: " + keys.size());
	    HashMap<String,Object> map = new HashMap<>();
	    if(keys.size()>0){
			try {
				Map<String, Object> cache = xMemcachedClient.get(keys);
				if(cache != null){
					map.putAll(cache);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }
        logger.info("getBulk end for keys " + keys.hashCode() + ", cached result size: " + map.size());
        return map;
	}

    @Override
	public void set(String key, Object value, int expiration) {
	    logger.info("set start for key: " + key+" expiration: "+expiration);
		try {
			xMemcachedClient.setWithNoReply(key, expiration, value);
		} catch (Exception e) {
			e.printStackTrace();
		}
	    logger.info("set end for key " + key);
	}

	@Override
	public void delete(String key) {
	    logger.info("delete start for key: " + key);
		if (key != null) {
			try {
				xMemcachedClient.deleteWithNoReply(key);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	    logger.info("delete end for key: " + key);
	}

	@Override
	public long incr(String key, long delta, int expiration) {
	    logger.info("incr start for key: " + key+" delta: "+delta+" expiration: "+expiration);
		long value = 0;
		if (key != null) {
			try {
				if (expiration > -1) {
					value = xMemcachedClient.incr(key, delta, 0, MemcachedClient.DEFAULT_OP_TIMEOUT, expiration);
				} else {
					value = xMemcachedClient.incr(key, delta, 0);
				}
			} catch (Exception e) {
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
			try {
				if (expiration > -1) {
					value = xMemcachedClient.decr(key, delta, 0, MemcachedClient.DEFAULT_OP_TIMEOUT, expiration);
				} else {
					value = xMemcachedClient.decr(key, delta, 0);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		logger.info("decr end for key: " + key);
		return value;
	}
	
	public static void main(String[] args) throws IOException {
        XmemcachedProxy proxy = new XmemcachedProxy();
        proxy.xMemcachedClient = new XMemcachedClient("54.255.142.109", 11211);
        proxy.set("k", "v");
        System.out.println(proxy.get("k"));
    }
}
