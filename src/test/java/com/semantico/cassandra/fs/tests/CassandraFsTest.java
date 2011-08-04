package com.semantico.cassandra.fs.tests;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.cassandra.contrib.fs.CassandraFileSystem;
import org.apache.cassandra.contrib.fs.Configuration;
import org.apache.cassandra.contrib.fs.FSConstants;
import org.apache.cassandra.contrib.fs.IFileSystem;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;

import com.semantico.sipp2.cassandra.tests.CassandraTest;

public class CassandraFsTest extends CassandraTest {

	protected IFileSystem fs;
	
	@Before
	public void setUpFS() throws IOException, TTransportException {
		Properties properties = new Properties();
		properties.load(new FileInputStream("config.properties"));
		properties.setProperty(FSConstants.KeySpace, keyspace.getKeyspaceName());
		properties.setProperty(FSConstants.ClusterName, cluster.describeClusterName());
		Configuration conf = new Configuration(properties);
		fs = CassandraFileSystem.getInstance(conf);
	}
}
