package org.apache.cassandra.contrib.fs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class Configuration {

	private final static Logger LOGGER = Logger.getLogger(Configuration.class);

	private Properties properties;

	public Configuration(String propertyFile) throws IOException {
		try {
			properties = new Properties();
			properties.load(new FileInputStream(propertyFile));
		} catch (FileNotFoundException e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * The properties object passed should not be modified post initialization of the Configuration object
	 * @param customProperties a custom properties object
	 */
	public Configuration(Properties customProperties) {
		this.properties = customProperties;
	}
	
	public Configuration() { //use all defaults
		properties = new Properties();
	}

	public String getHosts() {
		String hosts = properties.getProperty(FSConstants.HostsKey);
		if (hosts == null) {
			LOGGER.warn("'" + FSConstants.HostsKey+ "' is not provided, the default value will been used");
			return FSConstants.DefaultHosts;
		} else {
			return hosts;
		}
	}

	public int getMaxActive() {
		String maxActive = properties.getProperty(FSConstants.MaxActiveKey);
		if (maxActive == null) {
			LOGGER.warn("'" + FSConstants.MaxActiveKey+ "' is not provided, the default value will been used");
			return FSConstants.DefaultMaxActive;
		} else {
			return Integer.parseInt(maxActive);
		}
	}

	public int getMaxIdle() {
		String maxIdle = properties.getProperty(FSConstants.MaxIdleKey);
		if (maxIdle == null) {
			LOGGER.warn("'" + FSConstants.MaxIdleKey
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultMaxIdle;
		} else {
			return Integer.parseInt(maxIdle);
		}
	}

	public int getMaxWaitTimeWhenExhausted() {
		String maxWaitTimeWhenExhausted = properties
				.getProperty(FSConstants.MaxWaitTimeWhenExhaustedKey);
		if (maxWaitTimeWhenExhausted == null) {
			LOGGER.warn("'" + FSConstants.MaxWaitTimeWhenExhaustedKey
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultMaxWaitTimeWhenExhausted;
		} else {
			return Integer.parseInt(maxWaitTimeWhenExhausted);
		}
	}

	public int getCassandraThriftSocketTimeout() {
		String cassandraThriftSocketTimeout = properties.getProperty(FSConstants.CassandraThriftSocketTimeoutKey);
		if (cassandraThriftSocketTimeout == null) {
			LOGGER.warn("'" + FSConstants.CassandraThriftSocketTimeoutKey
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultCassandraThriftSocketTimeout;
		} else {
			return Integer.parseInt(cassandraThriftSocketTimeout);
		}
	}
	
	public String getReplicaPlacementStrategy() {
		String cassandraReplicaStrategyClass = properties.getProperty(FSConstants.ReplicaStrategyClassKey);
		if (cassandraReplicaStrategyClass == null) {
			LOGGER.warn("'" + FSConstants.ReplicaStrategyClassKey
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultReplicaPlacementStrategy;
		} else {
			return cassandraReplicaStrategyClass;
		}
	}
	
	public int getReplicationFactor() {
		String cassandraReplicationFactor = properties.getProperty(FSConstants.ReplicationFactorKey);
		if (cassandraReplicationFactor == null) {
			LOGGER.warn("'" + FSConstants.ReplicationFactorKey
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultReplicationFactor;
		} else {
			return Integer.parseInt(cassandraReplicationFactor);
		}
	}
	
	public String getClusterName() {
		String clusterName = properties.getProperty(FSConstants.ClusterNameKey);
		if(clusterName == null) {
			return FSConstants.DefaultClusterName;
		} else {
			return clusterName;
		}
	}
	
	public String getKeyspace() {
		String keyspace = properties.getProperty(FSConstants.KeySpaceKey);
		if(keyspace == null) {
			return FSConstants.DefaultKeySpace;
		} else {
			return keyspace;
		}
	}

	public int getBlockSize() {
		String fileBlockSize = properties.getProperty(FSConstants.BlockSizeKey);
		if (fileBlockSize == null) {
			LOGGER.warn("'" + FSConstants.BlockSizeKey
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultBlockSize;
		} else {
			return Integer.parseInt(fileBlockSize);
		}
	}

//	public ExhaustedPolicy getExhaustedPolicy() {
//		String exhaustedPolicy = properties
//				.getProperty(FSConstants.ExhaustedPolicy);
//		if (exhaustedPolicy == null) {
//			LOGGER.warn("'" + FSConstants.ExhaustedPolicy
//					+ "' is not provided, the default value will been used");
//			return defaultExhaustedPolicy;
//		} else {
//			return ExhaustedPolicy.valueOf(exhaustedPolicy);
//		}
//	}

}
