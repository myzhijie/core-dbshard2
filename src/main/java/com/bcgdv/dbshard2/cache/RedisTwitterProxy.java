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

import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.util.SerializationUtils;

import redis.clients.jedis.BinaryJedis;

public class RedisTwitterProxy extends RedisProxy {
	private static final Logger logger = Logger.getLogger(RedisTwitterProxy.class);

    @Override
    public CacheType getType() {
        return CacheType.redis_twemproxy;
    }
    
	@Override
	public void setBulk(Map<String, Object> entities, int expiration) {
        logger.info("setBulk start " + entities.hashCode()+" expiration: "+expiration);
		BinaryJedis jedis = getJedisFromPool();
        if(jedis != null) {
			try {
                for (Map.Entry<String, Object> entity : entities.entrySet()) {
                	byte[] mintKeyByteArray = SerializationUtils.serialize(entity.getKey());
                	byte[] valueByteArray = SerializationUtils.serialize(entity.getValue());
                	jedis.setex(mintKeyByteArray, expiration,valueByteArray);
                }
			} finally {
				returnJedisToPool(jedis);
			}
		}
        logger.info("setBulk end " + entities.hashCode());
	}

}