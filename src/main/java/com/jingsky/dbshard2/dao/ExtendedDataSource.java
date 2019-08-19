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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;

import com.jingsky.dbshard2.util.DateUtil;

public class ExtendedDataSource extends BasicDataSource {
	private static Logger logger = Logger.getLogger(ExtendedDataSource.class);
	
	private ThreadLocal<RequestContext> threadContext = new ThreadLocal<RequestContext>();
	private Map<RequestContext, Connection> connections = new HashMap<RequestContext, Connection>();

	private int dataSourceId;
	private boolean autoCommit;

	public void setThreadContext(RequestContext tc) {
		threadContext.set(tc);
	}
	
	public int getDataSourceId() {
		return dataSourceId;
	}

	public void setDataSourceId(int dataSourceId) {
		this.dataSourceId = dataSourceId;
	}
	
	@Override
	public Connection getConnection() throws SQLException {
	    long t0 = DateUtil.currentTimeMillis();
	    long t1 = 0;
        RequestContext context = getThreadContext().get();
		    t1 = DateUtil.currentTimeMillis() - t0;
		    if(t1 > 1000) {
		        System.out.println(context + " getConnection sync block costs " + t1);
		    }
			if(context != null) {
		        synchronized (context) {
		            Connection conn = null;
		            synchronized (connections) {
		                conn = connections.get(context);
		            }
        			if(conn == null) {
        				conn = super.getConnection();
        	            t1 = DateUtil.currentTimeMillis() - t0;
        	            if(t1 > 1000) {
        	                System.out.println(context + " super.getConnection costs " + t1);
        	            }
        				conn.setAutoCommit(isAutoCommit());
        		        synchronized (connections) {
        		            connections.put(getThreadContext().get(), conn);
        		        }
        				getThreadContext().get().addDataSource(this);
        				logger.debug(">>>>>>>>>>>dataSourceId " + dataSourceId + " request " + getThreadContext().get() + " connection " + conn.hashCode() + " create.");
        			}
        			else {
        				logger.debug(">>>>>>>>>>>dataSourceId " + dataSourceId + " request " + getThreadContext().get() + " connection " + conn.hashCode() + " reuse.");
        			}
                    t1 = DateUtil.currentTimeMillis() - t0;
                    if(t1 > 1000) {
                        System.out.println(context + " getConnection end costs " + t1);
                    }
                    return conn;
		        }
			}
			else {
                logger.debug(">>>>>>>>>>> No RequestContext found. set auto commit to true");
			    Connection conn = super.getConnection();
                conn.setAutoCommit(true);
                t1 = DateUtil.currentTimeMillis() - t0;
                if(t1 > 1000) {
                    System.out.println(context + " getConnection no requestcontext costs " + t1);
                }
                return conn;
			}
	}
	
	public void closeConnection(RequestContext rc) {
		Connection connection = null;
		synchronized (connections) {
			connection = connections.get(rc);
			connections.remove(rc);
		}
		logger.debug(">>>>>>>>>>>dataSourceId " + dataSourceId + " request " + rc + " connection " + connection.hashCode() + " close.");
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void commitConnection(RequestContext rc) {
		Connection connection = null;
		synchronized (connections) {
			connection = connections.get(rc);
		}
		logger.debug(">>>>>>>>>>>dataSourceId " + dataSourceId + " request " + rc + " connection " + connection.hashCode() + " commit.");
		try {
			connection.commit();
		} catch (SQLException e) {
			logger.warn("cannot commit", e);
		}
	}
	
	public void rollbackConnection(RequestContext rc) {
		Connection connection = null;
		synchronized (connections) {
			connection = connections.get(rc);
		}
		System.out.println(">>>>>>>>>>>dataSourceId " + dataSourceId + " request " + rc + " connection " + connection.hashCode() + " commit.");
		try {
			connection.rollback();;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

    public ThreadLocal<RequestContext> getThreadContext() {
        return threadContext;
    }

    public void setThreadContext(ThreadLocal<RequestContext> threadContext) {
        this.threadContext = threadContext;
    }
}
