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

package com.gaoshin.dao.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;

import com.jingsky.dbshard2.dao.DbDialet;
import com.jingsky.dbshard2.dao.DbShardUtils;
import com.jingsky.dbshard2.dao.ObjectId;
import com.jingsky.dbshard2.dao.ShardedDataSourceImpl;
import com.jingsky.dbshard2.dao.TableManager;
import com.jingsky.dbshard2.dao.entity.ObjectData;
import com.jingsky.dbshard2.dao.entity.TestUser;
import com.jingsky.dbshard2.dao.impl.BaseDaoImpl;
import com.jingsky.dbshard2.dao.impl.ShardResolverBase;
import com.jingsky.dbshard2.util.UuidUtil;
import com.jingsky.dbshard2.util.DateUtil;

public class BaseDaoImplDbTest {
	@Test
	public void test(){
		BaseDaoImpl baseDao = getDao("test");
		
		// test create
		ObjectData obj = createObjectData();
		final String id = obj.id;
		String json = obj.json;
		baseDao.create(obj);

		// test getData
		ObjectData data = baseDao.objectLookup(id);
		Assert.assertEquals(data.json, json);
		Assert.assertEquals(data.id, id);
		
		// test update
		json = UuidUtil.randomType1Uuid();
		obj.json = json ;
		baseDao.update(obj);
		data = baseDao.objectLookup(id);
		Assert.assertEquals(data.json, json);
		Assert.assertEquals(data.id, id);
		
		// test remove
		baseDao.delete(id);
		data = baseDao.objectLookup(id);
		Assert.assertNull(data);

		ObjectData obj2 = createObjectData();
		final String id2 = obj2.id;
		String json2 = obj2.json;
		baseDao.create(obj2);

		ObjectData obj3 = createObjectData();
		final String id3 = obj3.id;
		String json3 = obj3.json;
		baseDao.create(obj3);

		List<ObjectData> result = baseDao.objectLookup(new ArrayList<String>(){{
			add(id2);
			add(id3);
		}});
		Assert.assertEquals(2, result.size());
		
		if(id2.equals(result.get(0).id)) {
			Assert.assertEquals(id3, result.get(1).id);
			Assert.assertEquals(json2, result.get(0).json);
			Assert.assertEquals(json3, result.get(1).json);
		}
		else {
			Assert.assertEquals(id3, result.get(0).id);
			Assert.assertEquals(json2, result.get(1).json);
			Assert.assertEquals(json3, result.get(0).json);
		}
	}
	
	private ObjectData createObjectData() {
		ObjectData obj = new ObjectData();
		
		ObjectId objectId = new ObjectId();
		objectId.setType("usr");
		objectId.setShard(0);
		objectId.setUuid(UuidUtil.randomType1Uuid());
		obj.id = objectId.toString();
		
		obj.created = DateUtil.currentTimeMillis();
		obj.updated = DateUtil.currentTimeMillis();
		obj.version = 1;
		
		String json = String.valueOf(DateUtil.currentTimeMillis());
		obj.json = json;
		
		return obj;
	}
	
	private BaseDaoImpl getDao(String dbname) {
		BaseDaoImpl baseDao = new BaseDaoImpl();
//		baseDao.setRequestContext(new RequestContext());
		
		ShardedDataSourceImpl ds = new ShardedDataSourceImpl();
		ds.setDbClassName("org.h2.Driver");
		ds.setUserName("sa");
		ds.setUrl("jdbc:h2:mem:" + dbname+ "__DATASOURCEID__;MODE=MySQL;DB_CLOSE_ON_EXIT=FALSE");
		baseDao.setShardedDataSource(ds);
		
		ShardResolverBase shardResolver = new ShardResolverBase(0, 1);
		baseDao.setShardResolver(shardResolver);
		shardResolver.setNumberOfShards(1);
		
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		baseDao.setExecutorService(executorService);
		baseDao.setTableManager(new TableManager(Arrays.asList((Class)TestUser.class)));
		
		List<String> sqls = DbShardUtils.getSqls(TestUser.class, DbDialet.H2);
		for(String sql : sqls ) {
			baseDao.updateAll(sql );
		}
		
		return baseDao;
	}
	
}
