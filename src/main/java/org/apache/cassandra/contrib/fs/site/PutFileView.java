package org.apache.cassandra.contrib.fs.site;

import java.io.InputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.contrib.fs.CassandraFileSystem;

public class PutFileView extends AbstractCassandraFsView {

	@Override
	protected void handleRequest(HttpServletRequest request,HttpServletResponse response) throws Exception {
		String path = getPath(request);
		store(request.getInputStream(), path);
		writeSuccessful(response,path);
	}
	
	private String store(InputStream stream, String path) throws Exception{
		CassandraFileSystem.getInstance(null).createFile(path, stream);
		return path;
	}

}
