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

package com.jingsky.dbshard2.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class MultiTask {
	private List<Runnable> tasks;
	private int finished;
	
	public MultiTask() {
	}
	
	public MultiTask(List<Runnable> tasks) {
		this.tasks = tasks;
	}
	
	public void addTask(Runnable task) {
		if(tasks == null) {
			tasks = new ArrayList<Runnable>();
		}
		tasks.add(task);
	}
	
	public void execute(ExecutorService es) {
		List<TaskRunnable> list = new ArrayList<MultiTask.TaskRunnable>();
		for(Runnable r : tasks) {
			TaskRunnable wrapper = new TaskRunnable(r);
			es.execute(wrapper);
			list.add(wrapper);
		}
		synchronized (this) {
			while(finished < tasks.size()) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		for(TaskRunnable tr : list) {
			if(tr.e != null) {
				throw new RuntimeException(tr.e);
			}
		}
	}
	
	private class TaskRunnable implements Runnable {
		private Runnable actual;
		private Exception e;

		public TaskRunnable(Runnable r) {
			this.actual = r;
		}
		
		@Override
		public void run() {
			try {
				actual.run();
			} catch (Exception e) {
				this.e = e;
			}
			finally {
				synchronized (MultiTask.this) {
					finished++;
					if(finished >= tasks.size()) {
						MultiTask.this.notify();
					}
				}
			}
		}
		
	}
}
