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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;

import com.bcgdv.dbshard2.dao.ClassIndex;
import com.bcgdv.dbshard2.dao.ColumnValues;
import com.bcgdv.dbshard2.dao.DbDialet;
import com.bcgdv.dbshard2.dao.DbShardUtils;
import com.bcgdv.dbshard2.dao.RequestContext;
import com.bcgdv.dbshard2.dao.ShardedDataSourceImpl;
import com.bcgdv.dbshard2.dao.TableManager;
import com.bcgdv.dbshard2.dao.entity.IndexedData;
import com.bcgdv.dbshard2.dao.entity.TestAccount;
import com.bcgdv.dbshard2.dao.entity.TestUser;
import com.bcgdv.dbshard2.dao.impl.ExtendedDaoImpl;
import com.bcgdv.dbshard2.dao.impl.ShardResolverBase;

public class ExtendedDaoImplDbTest {
	@Test
	public void valueCombinationTest() {
		List<ColumnValues> columnValues = new ArrayList<ColumnValues>();
		columnValues.add(new ColumnValues("a", new ArrayList()));
		columnValues.get(0).values.add("a0");
		columnValues.get(0).values.add("a1");
		columnValues.add(new ColumnValues("b", new ArrayList()));
		columnValues.get(1).values.add("b0");
		columnValues.get(1).values.add("b1");
		columnValues.get(1).values.add("b2");
		List<Map> list = ExtendedDaoImpl.valueCombination(columnValues);
		
		List<String> expected = new ArrayList<String>();
		expected.add("a0b0");
		expected.add("a1b1");
		expected.add("a0b1");
		expected.add("a1b0");
		
		for(Map m : list) {
			Object a = m.get("a");
			Object b = m.get("b");
			expected.remove(a+""+b);
		}
		Assert.assertEquals(0, expected.size());
	}
	
	@Test
	public void test(){
		ExtendedDaoImpl dao = getDao("exttest");
		
		{
			TestUser user = new TestUser();
			user.setFirstName("f1");
			user.setLastName("l1");
			dao.createBean(user);
		}
		
		{
			TestUser user = new TestUser();
			user.setFirstName("f2");
			user.setLastName("l2");
			dao.createBean(user);
			
			TestUser db = dao.getBean(user.id);
	        Assert.assertEquals(user.getFirstName(), db.getFirstName());
		}
		
		Map values = new HashMap<String, Object>();
		values.put("firstName", "f1");
		values.put("lastName", "l1");
		ClassIndex index = dao.indexByKeys(TestUser.class, values.keySet());
		List<IndexedData> list = dao.indexedLookup(null, index, values);
		Assert.assertEquals(1, list.size());
		Assert.assertEquals("f1", list.get(0).get("firstName"));
		Assert.assertEquals("l1", list.get(0).get("lastName"));
	}
	
	private ExtendedDaoImpl getDao(String dbname) {
		ExtendedDaoImpl dao = new ExtendedDaoImpl(TestUser.class);
		dao.setRequestContext(new RequestContext());
		
		ShardedDataSourceImpl ds = new ShardedDataSourceImpl();
		ds.setDbClassName("org.h2.Driver");
		ds.setUserName("sa");
		ds.setUrl("jdbc:h2:mem:" + dbname+ "__DATASOURCEID__;MODE=MySQL;DB_CLOSE_ON_EXIT=FALSE");
		dao.setShardedDataSource(ds);
		
		ShardResolverBase shardResolver = new ShardResolverBase(0, 1);
		dao.setShardResolver(shardResolver);
		shardResolver.setNumberOfShards(1);
		
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		dao.setExecutorService(executorService);
		dao.setTableManager(new TableManager(Arrays.asList((Class)TestUser.class)));
		
		List<String> sqls = DbShardUtils.getSqls(TestUser.class, DbDialet.H2);
		for(String sql : sqls ) {
			dao.updateAll(sql );
		}
		
		return dao;
	}

	@Test
	public void testSameId() {
		ExtendedDaoImpl dao = new ExtendedDaoImpl(TestUser.class, TestAccount.class);
		TestUser user = new TestUser();
		ShardResolverBase shardResolver = new ShardResolverBase(0, 1);
		dao.setShardResolver(shardResolver);
		shardResolver.setNumberOfShards(1);
		
		String userId = dao.generateIdForBean(user);
		String accountId = dao.sameIdExceptType(userId, TestAccount.class);
		Assert.assertEquals(userId.substring(3), accountId.substring(3));
	}
}