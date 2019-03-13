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

package com.bcgdv.dbshard2.util.reflection;

import org.springframework.beans.BeanUtils;

public class CopyCat {
	public static Object copyProperties(Object src, Object dst, String... ignores) {
		if(src == null)
			return null;
		BeanUtils.copyProperties(src, dst, ignores);
		return dst;
	}
	
	public static <T> T copyProperties(Object src, Class<T> dstClass, String... ignores) {
		if(src == null)
			return null;
		T dst = null;
		try {
			dst = dstClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		BeanUtils.copyProperties(src, dst, ignores);
		return dst;
	}
}
