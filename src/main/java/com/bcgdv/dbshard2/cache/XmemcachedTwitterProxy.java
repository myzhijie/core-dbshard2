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

import net.rubyeye.xmemcached.MemcachedClient;

import org.apache.log4j.Logger;

public class XmemcachedTwitterProxy extends XmemcachedProxy {
	private static final Logger logger = Logger.getLogger(XmemcachedTwitterProxy.class);

	@Override
	public void setXMemcachedClient(MemcachedClient xMemcachedClient) {
	    super.setXMemcachedClient(xMemcachedClient);
        xMemcachedClient.setEnableHeartBeat(false);
	}

    @Override
    public CacheType getType() {
        return CacheType.xmemcached_twemproxy;
    }
    
	@Override
	public Object get(String key, int expiration) {
	    logger.info("get start for key: " + key+" expiration: "+expiration);
	    Object value = null;
		try {
		    value = xMemcachedClient.get(key);
        }catch (Exception e) {
            e.printStackTrace();
        }
	    logger.info("get end for key " + key);
        return value;
	}

}
