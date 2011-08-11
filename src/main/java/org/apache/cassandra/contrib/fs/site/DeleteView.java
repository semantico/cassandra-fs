package org.apache.cassandra.contrib.fs.site;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.contrib.fs.CassandraFileSystem;

public class DeleteView extends AbstractCassandraFsView {

	@Override
	protected void handleRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
		String path = getPath(request);
		delete(path);
    	writeSuccessful(response,path);
	}
	
	private void delete(String path) throws Exception {
		CassandraFileSystem.getInstance(null).deleteFile(path);
	}

}
