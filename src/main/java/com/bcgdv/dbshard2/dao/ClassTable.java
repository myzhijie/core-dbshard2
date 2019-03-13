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

public class ClassTable {
	private ShardedTable annotation;
	private Class forcls;
	
	public ClassTable() {
	}
	
	public ClassTable(Class forcls) {
		setForcls(forcls);
	}
	
	public Class getForcls() {
		return forcls;
	}
	
	public void setForcls(Class forcls) {
		this.forcls = forcls;
		setAnnotation((ShardedTable) forcls.getAnnotation(ShardedTable.class));
	}
	
	public Mapping getMapping(Class cls) {
		for(Mapping mapping : getAnnotation().mappings()) {
			if(mapping.map2cls().equals(cls))
				return mapping;
		}
		throw new RuntimeException(forcls.getSimpleName() + " has no mapping for " + cls);
	}
	
	public ClassMapping getClassMapping(Class cls) {
		Mapping mapping = getMapping(cls);
		return new ClassMapping(forcls, mapping);
	}

	public ShardedTable getAnnotation() {
		return annotation;
	}

	public void setAnnotation(ShardedTable annotation) {
		this.annotation = annotation;
	}
}
