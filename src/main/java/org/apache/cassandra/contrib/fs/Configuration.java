package org.apache.cassandra.contrib.fs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class ClientConfiguration {

	private final static Logger LOGGER = Logger
			.getLogger(ClientConfiguration.class);

	private Properties properties;

//	private CassandraClientPoolByHost.ExhaustedPolicy defaultExhaustedPolicy = CassandraClientPoolByHost.ExhaustedPolicy.WHEN_EXHAUSTED_BLOCK;

	private String defaultHosts = "localhost:9160";
	
	private String defaultReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy";
	private int defaultReplicationFactor = 1;

	private int defaultMaxActive = 10;

	private int defaultMaxIdle = 10;

	private int defaultMaxWaitTimeWhenExhausted = 60 * 1000;

	private int defaultCassandraThriftSocketTimeout = 60 * 1000;

	public ClientConfiguration(String propertyFile) throws IOException {
		try {
			properties = new Properties();
			properties.load(new FileInputStream(propertyFile));
		} catch (FileNotFoundException e) {
			throw new IOException(e);
		}
	}

	public String getHosts() {
		String hosts = properties.getProperty(FSConstants.Hosts);
		if (hosts == null) {
			LOGGER.warn("'" + FSConstants.Hosts
					+ "' is not provided, the default value will been used");
			return defaultHosts;
		} else {
			return hosts;
		}
	}

	public int getMaxActive() {
		String maxActive = properties.getProperty(FSConstants.MaxActive);
		if (maxActive == null) {
			LOGGER.warn("'" + FSConstants.MaxActive
					+ "' is not provided, the default value will been used");
			return defaultMaxActive;
		} else {
			return Integer.parseInt(maxActive);
		}
	}

	public int getMaxIdle() {
		String maxIdle = properties.getProperty(FSConstants.MaxIdle);
		if (maxIdle == null) {
			LOGGER.warn("'" + FSConstants.MaxIdle
					+ "' is not provided, the default value will been used");
			return defaultMaxIdle;
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
			return defaultMaxWaitTimeWhenExhausted;
		} else {
			return Integer.parseInt(maxWaitTimeWhenExhausted);
		}
	}

	public int getCassandraThriftSocketTimeout() {
		String cassandraThriftSocketTimeout = properties.getProperty(FSConstants.CassandraThriftSocketTimeout);
		if (cassandraThriftSocketTimeout == null) {
			LOGGER.warn("'" + FSConstants.CassandraThriftSocketTimeout
					+ "' is not provided, the default value will been used");
			return defaultCassandraThriftSocketTimeout;
		} else {
			return Integer.parseInt(cassandraThriftSocketTimeout);
		}
	}
	
	public String getReplicaPlacementStrategy() {
		String cassandraReplicaStrategyClass = properties.getProperty(FSConstants.ReplicaStrategyClass);
		if (cassandraReplicaStrategyClass == null) {
			LOGGER.warn("'" + FSConstants.ReplicaStrategyClass
					+ "' is not provided, the default value will been used");
			return defaultReplicaPlacementStrategy;
		} else {
			return cassandraReplicaStrategyClass;
		}
	}
	
	public int getReplicationFactor() {
		String cassandraReplicationFactor = properties.getProperty(FSConstants.ReplicationFactor);
		if (cassandraReplicationFactor == null) {
			LOGGER.warn("'" + FSConstants.ReplicationFactor
					+ "' is not provided, the default value will been used");
			return defaultReplicationFactor;
		} else {
			return Integer.parseInt(cassandraReplicationFactor);
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

	public static void main(String[] args) {
		Properties prop = new Properties();
		System.out.println(prop.getProperty("zjf"));
	}
}
