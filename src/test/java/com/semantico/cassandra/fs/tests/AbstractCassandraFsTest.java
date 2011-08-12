package com.semantico.cassandra.fs.tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.cassandra.contrib.fs.CassandraFacade;
import org.apache.cassandra.contrib.fs.CassandraFileSystem;
import org.apache.cassandra.contrib.fs.Configuration;
import org.apache.cassandra.contrib.fs.FSConstants;
import org.apache.cassandra.contrib.fs.IFileSystem;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import com.semantico.sipp2.cassandra.tests.CassandraTest;

public abstract class AbstractCassandraFsTest extends CassandraTest {

	protected IFileSystem fs;
	protected Configuration conf;
	 
	@Before
	public void setUpFS() throws IOException, TTransportException {
		Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
		cleanPrebuiltKeyspace();
		Properties properties = new Properties();
		//TODO: is the right path or is a location maven copies them to for testing ?
		properties.load(new FileInputStream(new File("src/test/resources/config.properties")));
		properties.setProperty(FSConstants.KeySpace, keyspace.getKeyspaceName());
		properties.setProperty(FSConstants.ClusterName, cluster.describeClusterName());
		properties.setProperty(FSConstants.HostsKey, "localhost:19160");
		conf = new Configuration(properties);
		fs = CassandraFileSystem.getInstance(conf);
		//logger.info("----------------- NEW KEYSPACE: " + keyspace.getKeyspaceName());
		
	}
	
	protected void cleanPrebuiltKeyspace() throws FileNotFoundException, IOException {
		cluster.dropKeyspace(keyspace.getKeyspaceName());
		CassandraFileSystem.dropInstance();//make sure we get a fresh copy
		CassandraFacade.dropInstance();
	}
}
