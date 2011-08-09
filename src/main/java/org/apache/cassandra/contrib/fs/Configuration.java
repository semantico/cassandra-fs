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
		String hosts = properties.getProperty(FSConstants.Hosts);
		if (hosts == null) {
			LOGGER.warn("'" + FSConstants.Hosts+ "' is not provided, the default value will been used");
			return FSConstants.DefaultHosts;
		} else {
			return hosts;
		}
	}

	public int getMaxActive() {
		String maxActive = properties.getProperty(FSConstants.MaxActive);
		if (maxActive == null) {
			LOGGER.warn("'" + FSConstants.MaxActive+ "' is not provided, the default value will been used");
			return FSConstants.DefaultMaxActive;
		} else {
			return Integer.parseInt(maxActive);
		}
	}

	public int getMaxIdle() {
		String maxIdle = properties.getProperty(FSConstants.MaxIdle);
		if (maxIdle == null) {
			LOGGER.warn("'" + FSConstants.MaxIdle
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultMaxIdle;
		} else {
			return Integer.parseInt(maxIdle);
		}
	}

	public int getMaxWaitTimeWhenExhausted() {
		String maxWaitTimeWhenExhausted = properties
				.getProperty(FSConstants.MaxWaitTimeWhenExhausted);
		if (maxWaitTimeWhenExhausted == null) {
			LOGGER.warn("'" + FSConstants.MaxWaitTimeWhenExhausted
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultMaxWaitTimeWhenExhausted;
		} else {
			return Integer.parseInt(maxWaitTimeWhenExhausted);
		}
	}

	public int getCassandraThriftSocketTimeout() {
		String cassandraThriftSocketTimeout = properties.getProperty(FSConstants.CassandraThriftSocketTimeout);
		if (cassandraThriftSocketTimeout == null) {
			LOGGER.warn("'" + FSConstants.CassandraThriftSocketTimeout
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultCassandraThriftSocketTimeout;
		} else {
			return Integer.parseInt(cassandraThriftSocketTimeout);
		}
	}
	
	public String getReplicaPlacementStrategy() {
		String cassandraReplicaStrategyClass = properties.getProperty(FSConstants.ReplicaStrategyClass);
		if (cassandraReplicaStrategyClass == null) {
			LOGGER.warn("'" + FSConstants.ReplicaStrategyClass
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultReplicaPlacementStrategy;
		} else {
			return cassandraReplicaStrategyClass;
		}
	}
	
	public int getReplicationFactor() {
		String cassandraReplicationFactor = properties.getProperty(FSConstants.ReplicationFactor);
		if (cassandraReplicationFactor == null) {
			LOGGER.warn("'" + FSConstants.ReplicationFactor
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultReplicationFactor;
		} else {
			return Integer.parseInt(cassandraReplicationFactor);
		}
	}
	
	public String getClusterName() {
		String clusterName = properties.getProperty(FSConstants.ClusterName);
		if(clusterName == null) {
			return FSConstants.DefaultClusterName;
		} else {
			return clusterName;
		}
	}
	
	public String getKeyspace() {
		String keyspace = properties.getProperty(FSConstants.KeySpace);
		if(keyspace == null) {
			return FSConstants.DefaultKeySpace;
		} else {
			return keyspace;
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
