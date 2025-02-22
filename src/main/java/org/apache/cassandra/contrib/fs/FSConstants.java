package org.apache.cassandra.contrib.fs;

import org.apache.cassandra.contrib.fs.util.Bytes;
/*
 * Notice: This file is modified from the original as provided under the apache 2.0 license
 */
/**
 * This class has a mix of actual constants and values that can overwitten by Configuration
 */
public class FSConstants {
	// ----------Property Keys---------------
	
	//client setup
	public final static String HostsKey = "cassandra.client.hosts";
	public final static String ExhaustedPolicyKey = ""; //Unsupported
	public final static String MaxActiveKey = "cassandra.client.maxActive";
	public final static String MaxIdleKey = "cassandra.client.maxIdle";
	public final static String MaxWaitTimeWhenExhaustedKey = "cassandra.client.maxWaitTimeWhenExhausted";
	public final static String CassandraThriftSocketTimeoutKey = "cassandra.client.cassandraThriftSocketTimeout";
	public final static String ReplicaStrategyClassKey = "cassandra.client.replicaPlacementStrategy"; 
	public final static String ReplicationFactorKey = "cassandra.client.replicationFactor";
	public final static String BlockSizeKey = "cassandra.client.blockSize";
	public final static String BufferLimitKey = "cassandra.client.bufferLimit";
	//cluster setup (should be consistent across nodes)
	public final static String ClusterNameKey = "cassandra.cluster.name";
	public final static String KeySpaceKey = "cassandra.cluster.keyspace";

	
	// -------------Defaults------------------
	// cluster setup
	public final static String DefaultHosts = "localhost:9160";
	public final static String DefaultClusterName = "FS_Cluster";
	public final static String DefaultKeySpace = "FS";
	public final static String DefaultFolderCF = "Folder";
	public final static String DefaultFileCF = "File";
	public final static String DefaultFolderFlag = "$_Folder_$";
	// client setup
	public final static String DefaultReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy";
	public final static int DefaultReplicationFactor = 1;
	public final static int DefaultMaxActive = 5;
	public final static int DefaultMaxIdle = 5;
	public final static int DefaultMaxWaitTimeWhenExhausted = 2 * 1000;
	public final static int DefaultCassandraThriftSocketTimeout = 5 * 1000;

	
	// attribute
	public final static String TypeAttr = "Type"; // file or folder
	public final static String ContentAttr = "Content"; //bytes
	public final static String LengthAttr = "Length"; //long (size in bytes)
	public final static String CompressedLengthAttr = "CompressedLength"; //long (size in bytes)
	public final static String LastModifyTime = "LastModifyTime";//Time
	public final static String OwnerAttr = "Owner";
	public final static String GroupAttr = "Group";

	// default owner and group

	public final static byte[] DefaultOwner = Bytes.toBytes("root");
	public final static byte[] DefaultGroup = Bytes.toBytes("supergroup");

	// size limitation
	public final static int MaxFileSize = 1024 * 1024 * 1024 * 1024; //1TB

	public final static int DefaultBlockSize = 1024*1024; //1mb


	
}
