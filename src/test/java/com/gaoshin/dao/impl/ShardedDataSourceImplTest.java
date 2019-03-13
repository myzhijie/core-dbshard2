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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import com.bcgdv.dbshard2.dao.ExtendedDataSource;
import com.bcgdv.dbshard2.dao.ObjectId;
import com.bcgdv.dbshard2.dao.RequestContext;
import com.bcgdv.dbshard2.dao.ShardedDataSourceImpl;

public class ShardedDataSourceImplTest {
	@Test
	public void test(){
		RequestContext tc = new RequestContext();
		
		ShardedDataSourceImpl sds = new ShardedDataSourceImpl();
		sds.setDbClassName("org.h2.Driver");
		sds.setUserName("sa");
		sds.setUrl("jdbc:h2:mem:test__DATASOURCEID__;MODE=MySQL;DB_CLOSE_ON_EXIT=FALSE");
		sds.setShardsPerDataSource(8);
		
		List<String> ids = new ArrayList<String>();
		for(int shardId=0; shardId<65; shardId++) {
			ExtendedDataSource eds = sds.getDataSourceByShardId(tc, shardId);
			Assert.assertEquals(shardId/8, eds.getDataSourceId());
			Assert.assertEquals("jdbc:h2:mem:test" + (shardId/8) + ";MODE=MySQL;DB_CLOSE_ON_EXIT=FALSE", eds.getUrl());
			
			ObjectId objectId = new ObjectId(shardId);
			objectId.setType("usr");
			ids.add(objectId.toString());
		}
		
		Map<Integer, List<String>> map = sds.splitByDataSource(ids);
		Assert.assertEquals(9, map.size());
		for(int i=0; i<9; i++) {
			Assert.assertNotNull(map.get(i));
			List<String> list = map.get(i);
			if(i==8)
				Assert.assertEquals(1, list.size());
			else
				Assert.assertEquals(8, list.size());
		}
		
		for(int dataSourceId=0; dataSourceId<65; dataSourceId++) {
			ExtendedDataSource eds = sds.getDataSourceByDataSourceId(tc, dataSourceId);
			Assert.assertEquals(dataSourceId, eds.getDataSourceId());
			Assert.assertEquals("jdbc:h2:mem:test" + dataSourceId + ";MODE=MySQL;DB_CLOSE_ON_EXIT=FALSE", eds.getUrl());
		}
		//test setShardDataSourceMappings
		sds.setShardDataSourceMappings("0-12,12-24");
		ExtendedDataSource eds = sds.getDataSourceByShardId(tc, 1);
		Assert.assertEquals(0, eds.getDataSourceId());
		eds = sds.getDataSourceByShardId(tc, 11);
		Assert.assertEquals(0, eds.getDataSourceId());
		eds = sds.getDataSourceByShardId(tc, 12);
		Assert.assertEquals(1, eds.getDataSourceId());
		eds = sds.getDataSourceByShardId(tc, 13);
		Assert.assertEquals(1, eds.getDataSourceId());
		eds = sds.getDataSourceByShardId(tc, 23);
		Assert.assertEquals(1, eds.getDataSourceId());
		eds = sds.getDataSourceByShardId(tc, 24);
		Assert.assertEquals(2, eds.getDataSourceId());
		eds = sds.getDataSourceByShardId(tc, 26);
		Assert.assertEquals(4, eds.getDataSourceId());

		try{
			sds.setShardDataSourceMappings("abc");
		}catch (Exception e){
			Assert.assertEquals("pass the shardDataSourceMappingsStr like 0-2,2-5,5-10", e.getMessage());
		}

		try{
			sds.setShardDataSourceMappings("1-0,12-24");
		}catch (Exception e){
			Assert.assertEquals("shardDataSourceMappings[0],start(1) must <= end (0)", e.getMessage());
		}

		try{
			sds.setShardDataSourceMappings("1-12,12-24");
		}catch (Exception e){
			Assert.assertEquals("shardDataSourceMappings[0],start(1) must =0", e.getMessage());
		}

		try{
			sds.setShardDataSourceMappings("0-12,13-24");
		}catch (Exception e){
			Assert.assertEquals("shardDataSourceMappings[1],start(13) must equals the end of pre :12",e.getMessage());
		}
	}
}
