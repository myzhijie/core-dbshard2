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

import org.apache.log4j.Logger;
import org.springframework.util.SerializationUtils;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

public class RedisProxy extends CacheProxyBase implements CacheProxy{
	private static final Logger logger = Logger.getLogger(RedisProxy.class);
	protected List<JedisPool> jedisPoolList = new ArrayList<JedisPool>();
	
	public void setServers(String servers) {
		jedisPoolList.clear();
	    if(servers != null && servers.length()>0){
		    for(String server : servers.split("[ ,;]+")) {
		        int pos = server.indexOf(":");
		        if(pos > 0){
			        String host = server.substring(0, pos);
			        int port = Integer.parseInt(server.substring(pos+1));
			        this.jedisPoolList.add(new JedisPool(host, port));
		        }
		    }
	    }
	}
	public void setJedisPoolList(List<JedisPool> jedisPoolList) {
		this.jedisPoolList.clear();
		if(jedisPoolList != null) this.jedisPoolList.addAll(jedisPoolList);
	}
	public List<JedisPool> getJedisPoolList() {
		return this.jedisPoolList;
	}
	
	protected BinaryJedis getJedisFromPool() {
        BinaryJedis binaryJedis = null;
	    synchronized (jedisPoolList) {
        	for(int i = 0; i<jedisPoolList.size();){
        		JedisPool jedisPool = jedisPoolList.get(i);
                try {
                	Jedis jedis = jedisPool.getResource();
                    if (jedis.isConnected()) {
                    	binaryJedis = jedis;
                        break;
                    }
                    i++;
                } catch (Exception e) {
                	e.printStackTrace();
                	jedisPoolList.remove(i);
                }
            }
        }
        return binaryJedis;
	}
	
	protected void returnJedisToPool(BinaryJedis jedis) {
		if(jedis != null){
	        synchronized (jedisPoolList) {
    			if(jedisPoolList.size() > 0) {
    				jedisPoolList.get(0).returnResource((Jedis) jedis);
    			}
	        }
		}
	}

    @Override
    public CacheType getType() {
        return CacheType.redis;
    }
    
	@Override
    public Object get(String key, int expiration) {
	    logger.info("get start for key: " + key+" expiration: "+expiration);
		Object value = null;
		if(key != null){
			BinaryJedis jedis = getJedisFromPool();
			if(jedis != null) {
				try {
					byte[] keyByteArray = SerializationUtils.serialize(key);
					value = SerializationUtils.deserialize(jedis.get(keyByteArray));
					if (expiration > -1) {
						jedis.expire(keyByteArray, expiration);
					}
				} finally {
					returnJedisToPool(jedis);
				}
			}
		}
        logger.info("get end for key " + key + (value == null ? ", not found" : ", found") );
		return value;
	}

	@Override
	public Map<String, Object> getBulk(Collection<String> keys) {
	    logger.info("getBulk start for keys " + keys.hashCode() + ". size: " + keys.size());
		Map<String, Object> values = new HashMap<String, Object>();
		if (keys.size() > 0) {
			BinaryJedis jedis = getJedisFromPool();
			if(jedis != null) {			
				try {
					byte[][] arrayMintKeysByteArray = getArrayOfByteArrays(keys);
					List<byte[]> listValuesByteArray = jedis.mget(arrayMintKeysByteArray);
					int i = 0;
					for (String key : keys) {
						if (listValuesByteArray.get(i) != null) {
							values.put(key, SerializationUtils.deserialize(listValuesByteArray.get(i)));
						}
						i++;
					}
				} finally {
					returnJedisToPool(jedis);
				}
			}
		}
        logger.info("getBulk end for keys " + keys.hashCode() + ", cached result size: " + values.size());
		return values;
	}

	@Override
	public void set(String key, Object value, int expiration) {
	    logger.info("set start for key: " + key+" expiration: "+expiration);
		BinaryJedis jedis = getJedisFromPool();
		if(jedis != null) {
			try {
				byte[] mintKeyByteArray = SerializationUtils.serialize(key);
				byte[] valueByteArray = SerializationUtils.serialize(value);
				jedis.setex(mintKeyByteArray, expiration,valueByteArray);
			} finally {
				returnJedisToPool(jedis);
			}
		}
	    logger.info("set end for key " + key);
	}

	@Override
	public void setBulk(Map<String, Object> entities, int expiration) {
        logger.info("setBulk start " + entities.hashCode()+" expiration: "+expiration);
		BinaryJedis jedis = getJedisFromPool();
		if(jedis != null) {
			try {
				Transaction t = jedis.multi();
				for (Map.Entry<String, Object> entity : entities.entrySet()) {
					byte[] mintKeyByteArray = SerializationUtils.serialize(entity.getKey());
					byte[] valueByteArray = SerializationUtils.serialize(entity.getValue());
					t.setex(mintKeyByteArray, expiration,valueByteArray);
				}
				t.exec();
			} finally {
				returnJedisToPool(jedis);
			}
		}
        logger.info("setBulk end " + entities.hashCode());
	}

	@Override
	public void delete(String key) {
	    logger.info("delete start for key: " + key);
		if (key != null) {
			BinaryJedis jedis = getJedisFromPool();
			if(jedis != null) {
				try {
					jedis.del(SerializationUtils.serialize(key));
				} finally {
					returnJedisToPool(jedis);
				}
			}
		}
	    logger.info("delete end for key: " + key);
	}

	@Override
	public void deleteBulk(Collection<String> keys) {
        logger.info("deleteBulk start " + keys.hashCode()+" "+keys.size()+" keys");
		BinaryJedis jedis = getJedisFromPool();
		if(jedis != null) {
			try {
				Transaction t = jedis.multi();
				for(byte[] arr :  getArrayOfByteArrays(keys))
				    t.del(arr);
				t.exec();
			} finally {
				returnJedisToPool(jedis);
			}
		}
		logger.info("deleteBulk end " + keys.hashCode());
	}
	
	@Override
    public long incr(String key, long delta, int expiration) {
	    logger.info("incr start for key: " + key+" delta: "+delta+" expiration: "+expiration);
		long value = 0;
		if(key != null){
			BinaryJedis jedis = getJedisFromPool();
			if(jedis != null) {
				try {
					byte[] keyByteArray = SerializationUtils.serialize(key);
					Long theValue = jedis.incrBy(keyByteArray, delta);
					if(theValue != null) {
						value = theValue.longValue();
						if (expiration > -1) {
							jedis.expire(keyByteArray, expiration);
						}
					}
				}finally {
					returnJedisToPool(jedis);
				}
			}
		}
		logger.info("incr end for key: " + key);
		return value;
	}
	
	@Override
    public long decr(String key, long delta, int expiration) {
	    logger.info("decr start for key: " + key+" delta: "+delta+" expiration: "+expiration);
		long value = 0;
		if(key != null){
			BinaryJedis jedis = getJedisFromPool();
			if(jedis != null) {
				try {
					byte[] keyByteArray = SerializationUtils.serialize(key);
					Long theValue = jedis.decrBy(keyByteArray, delta);
					if(theValue != null) {
						value = theValue.longValue();
						if (expiration > -1) {
							jedis.expire(keyByteArray, expiration);
						}
					}
				}finally {
					returnJedisToPool(jedis);
				}
			}
		}
		logger.info("decr end for key: " + key);
		return value;
	}

	private static final byte[][] getArrayOfByteArrays(Collection<?> objs) {
		byte[][] arrayByteArray = null;
		if (objs != null) {
			arrayByteArray = new byte[objs.size()][];
			int i = 0;
			for (Object obj : objs) {
				arrayByteArray[i++] = SerializationUtils.serialize(obj);
			}
		}
		return arrayByteArray;
	}
	
	public static void main(String[] args) {
        RedisProxy proxy = new RedisProxy();
        proxy.setServers("54.255.142.109:6379");
        proxy.set("k", "v");
        System.out.println(proxy.get("k"));
    }
}
