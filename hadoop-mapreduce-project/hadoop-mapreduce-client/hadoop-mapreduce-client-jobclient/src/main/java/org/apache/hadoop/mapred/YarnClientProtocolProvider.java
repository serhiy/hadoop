/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class YarnClientProtocolProvider extends ClientProtocolProvider {

  @Override
  public ClientProtocol create(Configuration conf) throws IOException {
    if (MRConfig.YARN_FRAMEWORK_NAME.equals(conf.get(MRConfig.FRAMEWORK_NAME))) {
    	/*Cluster.LOG.info("--- MPSR --- : create() : Yarn config"); 
    	try {
    		Cluster.LOG.info("--- MPSR --- : create() : Trying conf . . .");
    		Configuration c = new Configuration(conf);
    		Cluster.LOG.info("--- MPSR --- : create() : Trying yarn conf . . .");
    		YarnConfiguration yc = new YarnConfiguration(conf);
    		Cluster.LOG.info("--- MPSR --- : create() : conf succeded.");
    		
    	} catch (Exception e) {
    		Cluster.LOG.error("---> ", e);
    	}*/
    	//Cluster.LOG.info("--- MPSR --- : create() : RM delegate"); 
    	//return new ResourceMgrDelegate(new YarnConfiguration(conf));
    	Cluster.LOG.info("--- MPSR --- : create() : Yarn runner @@@."); 
    	return new YARNRunner(conf);
    }
    return null;
  }

  @Override
  public ClientProtocol create(InetSocketAddress addr, Configuration conf)
      throws IOException {
	  Cluster.LOG.info("--- MPSR --- : create() : Yarn runner with inet addr."); 
    return create(conf);
  }

  @Override
  public void close(ClientProtocol clientProtocol) throws IOException {
    if (clientProtocol instanceof YARNRunner) {
      ((YARNRunner)clientProtocol).close();
    }
  }
}
