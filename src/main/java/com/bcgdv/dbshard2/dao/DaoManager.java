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

package com.bcgdv.dbshard2.dao;

import java.util.HashMap;
import java.util.Map;

public class DaoManager {
	private static DaoManager instance = new DaoManager();
	public static DaoManager getInstance() {
		return instance;
	}
	
	private Map<Class, ExtendedDao> map = new HashMap<Class, ExtendedDao>();
	
	public void add(Class cls, ExtendedDao dao) {
		map.put(cls, dao);
	}

	public ExtendedDao get(Class cls) {
		return map.get(cls);
	}
}
