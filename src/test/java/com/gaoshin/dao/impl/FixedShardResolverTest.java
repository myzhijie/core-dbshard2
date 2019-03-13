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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.bcgdv.dbshard2.dao.impl.FixedShardResolver;

public class FixedShardResolverTest {
	@Test
	public void testFixedShardResolver() {
		final AtomicInteger ai = new AtomicInteger();
		
		FixedShardResolver fsr = new FixedShardResolver();
		fsr.setNumberOfShards(64);
		for(int i=0; i<1000; i++) {
			Object obj = new Object() {
				@Override
				public int hashCode() {
					return ai.getAndAdd(1);
				}
			};
			int shardId = fsr.getShardId(obj);
			Assert.assertEquals(i%64, shardId);
		}
	}
}
