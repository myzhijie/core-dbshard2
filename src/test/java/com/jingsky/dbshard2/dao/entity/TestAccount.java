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

package com.jingsky.dbshard2.dao.entity;

import com.jingsky.dbshard2.dao.DbDialet;
import com.jingsky.dbshard2.dao.DbShardUtils;
import com.jingsky.dbshard2.dao.Index;
import com.jingsky.dbshard2.dao.Mapping;
import com.jingsky.dbshard2.dao.ShardedTable;

@ShardedTable(
		type="acc", 
		indexes={@Index({"extId"})},
		mappings={@Mapping(column="userId", map2cls=TestUser.class, otherColumns={"type", "extId"})}
	)
public class TestAccount extends ObjectData {
	private String extId;
	private String type;
	private String userId;
	
	public String getUserId() {
		return userId;
	}
	
	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getExtId() {
		return extId;
	}

	public void setExtId(String extId) {
		this.extId = extId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	public static void main(String[] args) {
		for(String sql : DbShardUtils.getSqls(TestAccount.class, DbDialet.H2)) {
			System.out.println(sql);
		}
	}
}
