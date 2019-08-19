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

package com.jingsky.dbshard2.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;


public class ShardedDataSourceImpl implements ShardedDataSource {
	private static Logger logger = Logger.getLogger(ExtendedDataSource.class);

	private int shardsPerDataSource;
	private String userName;
	private String password;
	private String url;
	private String dbClassName;
	private int maxTotal = 10;
	private int maxIdle = 5;
	private boolean autoCommit = false;
	private String dbBaseName = null;
	private String validationQuery;
	private boolean testOnBorrow;
	
	private Map<Integer, ExtendedDataSource> dataSources;
	/**
	 * User-defined mapping of shard to dataSource,precedence over shardsPerDataSource.
	 */
	private Map<Integer, Integer> shardDataSourceMappings;
	/**
	 *	the dataSource size of user-defined mapping of shard to dataSource
	 */
	private int shardDataSourceMappingsSize;

	public ShardedDataSourceImpl() {
		setShardsPerDataSource(1);
		dataSources = new HashMap<Integer, ExtendedDataSource>();
	}

	@Override
	public ExtendedDataSource getDataSourceByShardId(RequestContext tc, int shardId) {
	    logger.debug("getDataSourceByShardId with RequestContext " + tc);
		Integer datasourceId =getDataSourceIdByShardId(shardId);
		return getDataSourceByDataSourceId(tc, datasourceId);
	}

	/**
	 * getDataSourceId ByShardId not DataSource url
	 * @param shardId
	 * @return int
     */
	private int getDataSourceIdByShardId(int shardId){
		Integer datasourceId = shardId / getShardsPerDataSource();
		if(shardDataSourceMappings!=null && shardDataSourceMappings.size()>0){
			datasourceId=shardDataSourceMappings.get(shardId);
			if(datasourceId==null){
				datasourceId=shardId-shardDataSourceMappings.size()+shardDataSourceMappingsSize;
			}
		}
		return datasourceId;
	}

	@Override
	public Map<Integer, List<String>> splitByDataSource(Collection<String> ids) {
		Map<Integer, List<String>> map = new HashMap<Integer, List<String>>();
		for (String s : ids) {
			ObjectId oi = new ObjectId(s);
			int shard = oi.getShard();
			int datasource =getDataSourceIdByShardId(shard);;
			List<String> list = map.get(datasource);
			if(list == null) {
				list = new ArrayList<String>();
				map.put(datasource, list);
			}
			list.add(s);
		}
		return map;
	}
	
	private String getUrlForDataSource(int dataSourceId) {
	    if(url.indexOf(" ") != -1) {
	        String[] urls = url.split("[ \t]+");
	        return urls[dataSourceId];
	    }
	    else 
	        return (url != null? url.replaceAll("__DATASOURCEID__", String.valueOf(dataSourceId)):url);
	}

	@Override
	public synchronized ExtendedDataSource getDataSourceByDataSourceId(RequestContext tc, int dataSourceId) {
        ExtendedDataSource ds = null;
        boolean newDataSource = false;
        
        synchronized (dataSources) {
            ds = dataSources.get(dataSourceId);
            if(ds == null) {
                ds = new ExtendedDataSource();
                dataSources.put(dataSourceId, ds);
                newDataSource = true;
            }
        }
        
        synchronized (ds) {
            if(newDataSource) {
                ds.setDriverClassName(dbClassName);
                ds.setUsername(userName);
                ds.setPassword(password);
                String dburl = getUrlForDataSource(dataSourceId);
                if(dbBaseName != null)
                    dburl = dburl.replaceAll("DBBASENAME", dbBaseName);
                ds.setUrl(dburl);
                logger.info(">>>>>>>> create data source for " + dburl);
                
                ds.setLogAbandoned(true);
                ds.setRemoveAbandonedOnBorrow(true);
                ds.setRemoveAbandonedOnMaintenance(true);
                ds.setRemoveAbandonedTimeout(300);
                ds.setMinEvictableIdleTimeMillis(300000);
                ds.setTimeBetweenEvictionRunsMillis(30000);
                ds.setMaxTotal(getMaxTotal());
                ds.setMaxIdle(getMaxIdle());
                ds.setMaxWaitMillis(2000);
                ds.setMaxOpenPreparedStatements(500);
                ds.setPoolPreparedStatements(false);
                ds.setAutoCommit(isAutoCommit());
                ds.setDataSourceId(dataSourceId);
                ds.setValidationQuery(getValidationQuery());
                ds.setTestOnBorrow(isTestOnBorrow());
            }
        }

		ds.setThreadContext(tc);
		return ds;
	}

	@Override
	public ExtendedDataSource getDataSourceByObjectId(RequestContext tc, String id) {
		ObjectId oi = new ObjectId(id);
		return getDataSourceByShardId(tc, oi.getShard());
	}

	/**
	 * User-defined mapping of shard to dataSource,precedence over shardsPerDataSource.
	 * @param shardDataSourceMappingsStr eg. 0-2,2-5,5-10
	 */
	public void setShardDataSourceMappings(String shardDataSourceMappingsStr) {
		//Using regular expressions ((\d+)\-(\d+),)+ judgment
		Pattern pattern = Pattern.compile("((\\d+)\\-(\\d+),)+");
		if(!pattern.matcher(shardDataSourceMappingsStr+",").matches()){
			throw new RuntimeException("pass the shardDataSourceMappingsStr like 0-2,2-5,5-10");
		}
		String [] mappingsArr=shardDataSourceMappingsStr.split(",");
		this.shardDataSourceMappingsSize=mappingsArr.length;
		this.shardDataSourceMappings=new HashMap<>();

		//the dateSource index
		int dateSourceIndex=0;
		//the end of pre shardArr
		int preEnd=0;
		for(String mapOne : mappingsArr){
			String[] shardArr=mapOne.split("-");
			int start=Integer.parseInt(shardArr[0]);
			int end=Integer.parseInt(shardArr[1]);
			if(start>end){
				throw new RuntimeException("shardDataSourceMappings["+dateSourceIndex+"],start("+start+") must <= end ("+end+")");
			}
			if(dateSourceIndex==0 && start!=0){
				throw new RuntimeException("shardDataSourceMappings["+dateSourceIndex+"],start("+start+") must =0");
			}
			if(dateSourceIndex>0 && preEnd!=start){
				throw new RuntimeException("shardDataSourceMappings["+dateSourceIndex+"],start("+start+") must equals the end of pre :"+preEnd);
			}
			for(int k=start ; k<end ; k++) {
				this.shardDataSourceMappings.put(k,dateSourceIndex);
			}
			preEnd=end;
			dateSourceIndex++;
		}
	}

	public int getMaxTotal() {
		return maxTotal;
	}

	public void setMaxTotal(int maxTotal) {
		this.maxTotal = maxTotal;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	public String getDbBaseName() {
		return dbBaseName;
	}

	public void setDbBaseName(String dbBaseName) {
		this.dbBaseName = dbBaseName;
	}

	public String getValidationQuery() {
		return validationQuery;
	}

	public void setValidationQuery(String validationQuery) {
		this.validationQuery = validationQuery;
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDbClassName() {
		return dbClassName;
	}

	public void setDbClassName(String dbClassName) {
		this.dbClassName = dbClassName;
	}

	public int getShardsPerDataSource() {
		return shardsPerDataSource;
	}

	public void setShardsPerDataSource(int shardsPerDataSource) {
		this.shardsPerDataSource = shardsPerDataSource;
	}
}
