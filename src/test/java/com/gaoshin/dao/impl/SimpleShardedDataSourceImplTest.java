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

import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.bcgdv.dbshard2.dao.RequestContext;
import com.bcgdv.dbshard2.dao.ShardedDataSourceImpl;

public class SimpleShardedDataSourceImplTest {
	@Test
	public void testDataSource() throws SQLException {
		RequestContext tc = new RequestContext();
		
		ShardedDataSourceImpl ds = new ShardedDataSourceImpl();
		ds.setDbClassName("org.h2.Driver");
		ds.setUserName("sa");
		ds.setUrl("jdbc:h2:mem:test__DATASOURCEID__;MODE=MySQL;DB_CLOSE_ON_EXIT=FALSE");

		DataSource ds0 = ds.getDataSourceByShardId(tc, 0);
		JdbcTemplate jt0 = new JdbcTemplate(new SingleConnectionDataSource(ds0.getConnection(), true));
		jt0.execute("create table test (id varchar(100) primary key)");
		jt0.execute("insert into test (id) values (1)");

		DataSource ds1 = ds.getDataSourceByShardId(tc, 1);
		JdbcTemplate jt1 = new JdbcTemplate(new SingleConnectionDataSource(ds1.getConnection(), true));
		jt1.execute("create table test (id varchar(100) primary key)");
		jt1.execute("insert into test (id) values (1)");
		
	}
}


