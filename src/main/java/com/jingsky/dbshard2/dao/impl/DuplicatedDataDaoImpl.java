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

package com.jingsky.dbshard2.dao.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import com.jingsky.dbshard2.util.MultiTask;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.jingsky.dbshard2.dao.DuplicatedDataDao;
import com.jingsky.dbshard2.dao.ExtendedDataSource;
import com.jingsky.dbshard2.dao.RequestAware;
import com.jingsky.dbshard2.dao.RequestContext;
import com.jingsky.dbshard2.dao.ShardResolver;
import com.jingsky.dbshard2.dao.ShardedDataSource;

public abstract class DuplicatedDataDaoImpl implements DuplicatedDataDao, RequestAware {
	public ShardResolver shardResolver;
	public ExecutorService executorService;
	public ShardedDataSource shardedDataSource;
	
	private ThreadLocal<RequestContext> threadContext = new ThreadLocal<RequestContext>();
	
	@Override
	public void setRequestContext(RequestContext tc) {
		threadContext.set(tc);
	}
	
	public ShardResolver getShardResolver() {
		return shardResolver;
	}

	public void setShardResolver(ShardResolver shardResolver) {
		this.shardResolver = shardResolver;
	}

	public ExecutorService getExecutorService() {
		return executorService;
	}

	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}

	public ShardedDataSource getShardedDataSource() {
		return shardedDataSource;
	}

	public void setShardedDataSource(ShardedDataSource shardedDataSource) {
		this.shardedDataSource = shardedDataSource;
	}

	public void forEachDataSource(ShardRunnable runnable){
		MultiTask mt = new MultiTask();
		for(int i= 0; i<shardResolver.getNumberOfShards() / shardedDataSource.getShardsPerDataSource(); i++) {
			ShardTask shardTask = new ShardTask(i, runnable);
			mt.addTask(shardTask);
		}
		mt.execute(executorService);
	}
	
	public void forSelectDataSources(Collection<Integer> dataSourceIds, ShardRunnable runnable){
		MultiTask mt = new MultiTask();
		for(Integer dataSourceId : dataSourceIds) {
			ShardTask shardTask = new ShardTask(dataSourceId, runnable);
			mt.addTask(shardTask);
		}
		mt.execute(executorService);
	}

	@Override
	public void update(final String sql, final Object... args) {
		forEachDataSource(new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				JdbcTemplate jt = new JdbcTemplate(dataSource);
				jt.update(sql, args);
			}
		});
	}
	
	@Override
	public void update(final String sql, final Map<String, Object> args) {
		forEachDataSource(new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				NamedParameterJdbcTemplate jt = new NamedParameterJdbcTemplate(dataSource);
				jt.update(sql, args);
			}
		});
	}
	
	public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object...args) {
		int shardId = new Random().nextInt(shardResolver.getNumberOfShards());
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByShardId(threadContext.get(), shardId);
		JdbcTemplate jt = new JdbcTemplate(dataSource);
		return jt.query(sql, args, rowMapper);
	}
	
	public <T> List<T> query(String sql, RowMapper<T> rowMapper, Map<String, Object>args) {
		int shardId = new Random().nextInt(shardResolver.getNumberOfShards());
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByShardId(threadContext.get(), shardId);
		NamedParameterJdbcTemplate jt = new NamedParameterJdbcTemplate(dataSource);
		return jt.query(sql, args, rowMapper);
	}
}
