package org.apache.cassandra.contrib.fs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Encapsulates access to the configuration of a CassandraFileSystem
 * If the requested configuration option has not been set then then configuration object falls back on defaults
 *
 */
public class Configuration {

	private final static Logger LOGGER = Logger.getLogger(Configuration.class);
	private Properties properties;

	/**
	 * Creates a configuration from a given properties file
	 * @param propertyFile the path to a properties file
	 * @throws IOException
	 */
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

	/**
	 * Makes a new configuration that relies upon default values
	 */
	public Configuration() { //use all defaults
		properties = new Properties();
		LOGGER.warn("Default configuration used");
	}

	/**
	 * 
	 * @return A list of hosts seperated by ',' e.g. 'localhost:9160,192.168.0.1:8080'
	 */
	public String getHosts() {
		String hosts = properties.getProperty(FSConstants.HostsKey);
		if (hosts == null) {
			LOGGER.warn("'" + FSConstants.HostsKey+ "' is not provided, the default value will be used");
			return FSConstants.DefaultHosts;
		}
		return hosts;

	}

	/**
	 * 
	 * @return The maximum number of client threads that will be active at any one time
	 */
	public int getMaxActive() {
		String maxActive = properties.getProperty(FSConstants.MaxActiveKey);
		if (maxActive == null) {
			LOGGER.warn("'" + FSConstants.MaxActiveKey+ "' is not provided, the default value will be used");
			return FSConstants.DefaultMaxActive;
		}
		return Integer.parseInt(maxActive);

	}

	/**
	 * 
	 * @return The maximum number of idle client threads there can be before they start being shut down to reduce memory consumption
	 */
	public int getMaxIdle() {
		String maxIdle = properties.getProperty(FSConstants.MaxIdleKey);
		if (maxIdle == null) {
			LOGGER.warn("'" + FSConstants.MaxIdleKey
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultMaxIdle;
		}
		return Integer.parseInt(maxIdle);

	}

	/**
	 * 
	 * @return the maximum time in miliseconds a request can be held up by lack of client threads before it will fail
	 */
	public int getMaxWaitTimeWhenExhausted() {
		String maxWaitTimeWhenExhausted = properties
				.getProperty(FSConstants.MaxWaitTimeWhenExhaustedKey);
		if (maxWaitTimeWhenExhausted == null) {
			LOGGER.warn("'" + FSConstants.MaxWaitTimeWhenExhaustedKey
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultMaxWaitTimeWhenExhausted;
		}
		return Integer.parseInt(maxWaitTimeWhenExhausted);

	}

	/**
	 * 
	 * @return how long in ms before a thrift socket will timeout
	 */
	public int getCassandraThriftSocketTimeout() {
		String cassandraThriftSocketTimeout = properties.getProperty(FSConstants.CassandraThriftSocketTimeoutKey);
		if (cassandraThriftSocketTimeout == null) {
			LOGGER.warn("'" + FSConstants.CassandraThriftSocketTimeoutKey
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultCassandraThriftSocketTimeout;
		}
		return Integer.parseInt(cassandraThriftSocketTimeout);

	}

	/**
	 * 
	 * @return the package+classname of the strategy to be used to place replicas on the ring
	 */
	public String getReplicaPlacementStrategy() {
		String cassandraReplicaStrategyClass = properties.getProperty(FSConstants.ReplicaStrategyClassKey);
		if (cassandraReplicaStrategyClass == null) {
			LOGGER.warn("'" + FSConstants.ReplicaStrategyClassKey
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultReplicaPlacementStrategy;
		}
		return cassandraReplicaStrategyClass;
	}

	/**
	 * Gets replication factor:
	 * 1 means there are no dubplicates, 2 means there are two copies of the information etc ...
	 * @return how many copies of each peice of data are made
	 */
	public int getReplicationFactor() {
		String cassandraReplicationFactor = properties.getProperty(FSConstants.ReplicationFactorKey);
		if (cassandraReplicationFactor == null) {
			LOGGER.warn("'" + FSConstants.ReplicationFactorKey
					+ "' is not provided, the default value will been used");
			return FSConstants.DefaultReplicationFactor;
		}
		return Integer.parseInt(cassandraReplicationFactor);

	}

	/**
	 * 
	 * @return The name of the cluster to connect to (used to ensure connection to the correct cluster when multiple rings overlap on physical nodes)
	 */
	public String getClusterName() {
		String clusterName = properties.getProperty(FSConstants.ClusterNameKey);
		if(clusterName == null) {
			return FSConstants.DefaultClusterName;
		}
		return clusterName;

	}

	/**
	 * 
	 * @return The name of the keyspace (database) to store cassandraFS data in
	 */
	public String getKeyspace() {
		String keyspace = properties.getProperty(FSConstants.KeySpaceKey);
		if(keyspace == null) {
			return FSConstants.DefaultKeySpace;
		}
		return keyspace;

	}

	/**
	 * 
	 * @return the size in bytes that a file should be split into
	 */
	public int getBlockSize() {
		String fileBlockSize = properties.getProperty(FSConstants.BlockSizeKey);
		if (fileBlockSize == null) {
			LOGGER.warn("'" + FSConstants.BlockSizeKey
					+ "' is not provided, the default value will be used");
			return FSConstants.DefaultBlockSize;
		}
		return Integer.parseInt(fileBlockSize);

	}

	/**
	 * 
	 * @return the total number of bytes that clients will used to buffer cassandra requests
	 */
	public int getBufferedMemoryLimit() {
		String bufferLimit = properties.getProperty(FSConstants.BufferLimitKey);
		if(bufferLimit == null) {
			return -1;
		}
		return Integer.parseInt(bufferLimit);
	}

	/**
	 * 
	 * @return whether the max active property has been configured or will use defaults
	 */
	public boolean isSetMaxActive() {
		return properties.getProperty(FSConstants.MaxActiveKey) != null;
	}

	/**
	 * 
	 * @return whether the max idle property has been configured or will use defaults
	 */
	public boolean isSetMaxIdle() {
		return properties.getProperty(FSConstants.MaxIdleKey) != null;
	}

	/**
	 * 
	 * @return whether the memory limit property has been set or will need to be derived
	 * @see setMemoryLimitFromMaxActive
	 */
	public boolean isSetMemoryLimit() {
		return properties.getProperty(FSConstants.BufferLimitKey) != null;
	}

	/**
	 * Sets the buffer memory limit parameter from the max active and block size
	 * memoryBufferUsed = maxActiveClientThreads*FileBlockSize
	 */
	public void setMemoryLimitFromMaxActive() {
		int active = getMaxActive();
		int blockSize = getBlockSize();
		properties.setProperty(FSConstants.BufferLimitKey, active*blockSize + "");
	}

	/**
	 * Sets the max active parameter from the memory limit and block size
	 * maxActiveClientThreads = memoryBufferLimit/fileBlockSize
	 */
	public void setMaxActiveFromMemoryLimit() {
		int memLimit = getBufferedMemoryLimit();
		int blockSize = getBlockSize();
		int active = memLimit/blockSize;
		if(active < 1) {
			LOGGER.warn("Buffer Memory Limit set too low, increase limit or decrease block size. potential OOM errors");
			active = 1;
		}
		properties.setProperty(FSConstants.MaxActiveKey, active + "");
	}
	
	/**
	 * Sets the maxIdle parameter from the maxActive parameter
	 * half of the client threads will be allowed to be idle (rounded up)
	 */
	public void setMaxIdleFromMaxActive() {
		properties.setProperty(FSConstants.MaxIdleKey,(getMaxActive()+1)/2 + "");
	}

}
