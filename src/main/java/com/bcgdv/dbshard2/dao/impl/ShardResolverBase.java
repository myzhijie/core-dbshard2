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

package com.bcgdv.dbshard2.dao.impl;

import java.util.Random;

import com.bcgdv.dbshard2.dao.ShardResolver;

public class ShardResolverBase<T> implements ShardResolver<T> {
	private Random random;
	private int minShardId4Write;
	private int maxShardId4Write;
	private int numberOfShards;

	public ShardResolverBase() {
		random = new Random();
	}
	
	public ShardResolverBase(int minShardId4Write, int maxShardId4Write) {
		random = new Random();
		this.minShardId4Write = minShardId4Write;
		this.maxShardId4Write = maxShardId4Write;
	}
	
	public int getMinShardId4Write() {
		return minShardId4Write;
	}

	public void setMinShardId4Write(int minShardId4Write) {
		this.minShardId4Write = minShardId4Write;
	}

	public int getMaxShardId4Write() {
		return maxShardId4Write;
	}

	public void setMaxShardId4Write(int maxShardId4Write) {
		this.maxShardId4Write = maxShardId4Write;
	}
	
	@Override
	public int getShardId(T obj) {
		return random.nextInt(maxShardId4Write - minShardId4Write) + minShardId4Write;
	}

	public int getNumberOfShards() {
		return numberOfShards;
	}

	public void setNumberOfShards(int numberOfShards) {
		this.numberOfShards = numberOfShards;
	}
	
}
