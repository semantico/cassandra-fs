package com.semantico.cassandra.fs.tests;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Properties;

import org.apache.cassandra.contrib.fs.Configuration;
import org.apache.cassandra.contrib.fs.FSConstants;
import org.junit.Test;

public class ConfigurationTest {

	@Test
	public void configWithFileTest() throws IOException {
		assertNotNull(new Configuration("src/test/resources/config.properties"));
	}
	
	@Test(expected=IOException.class)
	public void failingConfigWithFileTest() throws IOException {
		new Configuration("blablabla");
	}
	
	@Test
	public void defaultConfigTest() {
		Properties customProperties = new Properties();
		Configuration config = new Configuration(customProperties);
		assertEquals(FSConstants.DefaultClusterName, config.getClusterName());
		assertEquals(FSConstants.DefaultHosts, config.getHosts());
		assertEquals(FSConstants.DefaultMaxActive, config.getMaxActive());
		assertEquals(FSConstants.DefaultMaxIdle, config.getMaxIdle());
		assertEquals(FSConstants.DefaultMaxWaitTimeWhenExhausted, config.getMaxWaitTimeWhenExhausted());
		assertEquals(FSConstants.DefaultCassandraThriftSocketTimeout, config.getCassandraThriftSocketTimeout());
		assertEquals(FSConstants.DefaultReplicaPlacementStrategy, config.getReplicaPlacementStrategy());
		assertEquals(FSConstants.DefaultReplicationFactor, config.getReplicationFactor());
		assertEquals(FSConstants.DefaultKeySpace, config.getKeyspace());
	}
	
	@Test
	public void nonDefaultConfigTest() {
		Properties customProperties = new Properties();
		String clusterName = "clusterName";
		String hosts = "hosts";
		int maxActive = 12;
		int maxIdle = 2;
		int maxWaitTimeWhenExhausted = 300;
		int thriftSocketTimeout = 400;
		String replicaStrategyClass = "some.class";
		int replicationFactor = 3;
		String keyspace = "ksp";
		customProperties.put(FSConstants.ClusterNameKey, clusterName);
		customProperties.put(FSConstants.HostsKey, hosts);
		customProperties.put(FSConstants.MaxActiveKey, maxActive+"");
		customProperties.put(FSConstants.MaxIdleKey, maxIdle+"");
		customProperties.put(FSConstants.MaxWaitTimeWhenExhaustedKey, maxWaitTimeWhenExhausted+"");
		customProperties.put(FSConstants.CassandraThriftSocketTimeoutKey, thriftSocketTimeout+"");
		customProperties.put(FSConstants.ReplicaStrategyClassKey, replicaStrategyClass);
		customProperties.put(FSConstants.ReplicationFactorKey, replicationFactor+"");
		customProperties.put(FSConstants.KeySpaceKey, keyspace);
		Configuration config = new Configuration(customProperties);
		assertEquals(clusterName, config.getClusterName());
		assertEquals(hosts, config.getHosts());
		assertEquals(maxActive, config.getMaxActive());
		assertEquals(maxIdle, config.getMaxIdle());
		assertEquals(maxWaitTimeWhenExhausted, config.getMaxWaitTimeWhenExhausted());
		assertEquals(thriftSocketTimeout, config.getCassandraThriftSocketTimeout());
		assertEquals(replicaStrategyClass, config.getReplicaPlacementStrategy());
		assertEquals(replicationFactor, config.getReplicationFactor());
		assertEquals(keyspace, config.getKeyspace());
	}
}
