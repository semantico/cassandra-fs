package com.semantico.cassandra.fs.tests;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.contrib.fs.CassandraFacade;
import org.apache.cassandra.contrib.fs.FSConstants;
import org.junit.Test;

public class CassandraFacadeTest extends AbstractCassandraFsTest {
	
	@Test
	public void configExistsTest() throws IOException {
		assertNotNull(CassandraFacade.getInstance(null));
		assertNotNull(CassandraFacade.getInstance(null).getConf());
	}
	
	@Test
	public void singletonTest() throws IOException {
		CassandraFacade instance1 = CassandraFacade.getInstance(conf);
		CassandraFacade instance2 = CassandraFacade.getInstance(conf);
		assertEquals(instance1, instance2);
		CassandraFacade.dropInstance();
		CassandraFacade instance3 = CassandraFacade.getInstance(conf);
		assertNotSame(instance1, instance3);
	}
	
	@Test(expected=IOException.class)
	public void IOExceptionTest1() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		cluster.dropKeyspace(conf.getKeyspace());
		facade.delete("bla");
	}
	
	@Test(expected=IOException.class)
	public void IOExceptionTest2() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		cluster.dropKeyspace(conf.getKeyspace());
		facade.delete("bla", "colFamily", "superCol");
	}
	
	@Test(expected=IOException.class)
	public void IOExceptionTest3() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		cluster.dropKeyspace(conf.getKeyspace());
		HashMap<byte[], byte[]> map = new HashMap<byte[], byte[]>();
		map.put("string".getBytes(), "moreString".getBytes());
		facade.batchPut("aKey", FSConstants.DefaultFileCF, "colname", map, false);
	}
	
	@Test(expected=IOException.class)
	public void IOExceptionTest4() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		cluster.dropKeyspace(conf.getKeyspace());
		facade.exist("bla");
	}
	
	@Test(expected=IOException.class)
	public void IOExceptionTest5() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		cluster.dropKeyspace(conf.getKeyspace());
		facade.listFile("bla");
	}
	
	@Test(expected=IOException.class)
	public void IOExceptionTest6() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		cluster.dropKeyspace(conf.getKeyspace());
		facade.list("key", "columnFamily", false);
	}
	
	@Test(expected=IOException.class)
	public void IOExceptionTest7() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		cluster.dropKeyspace(conf.getKeyspace());
		facade.get("key", "");
	}
	
	@Test(expected=IOException.class)
	public void IOExceptionTest8() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		cluster.dropKeyspace(conf.getKeyspace());
		facade.put("key", "col", "value".getBytes());
	}
	
	@Test(expected=IOException.class)
	public void incorrectColumnPathTest() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		facade.get("foo", "bar");
	}
	
	@Test
	public void listFileTest() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		assertNotNull(facade.listFile("someFile"));
	}
	
	@Test(expected=IOException.class)
	public void listWrongPathTest() throws IOException {
		CassandraFacade facade = CassandraFacade.getInstance(conf);
		facade.list("key", "columnFamilyThatIsntSupported", true);
	}

}
