package org.apache.cassandra.contrib.fs.site;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.contrib.fs.CassandraFileSystem;
import org.apache.commons.io.IOUtils;
import org.apache.thrift.transport.TTransportException;

public class GetFileView extends AbstractCassandraFsView{
	
	@Override
	protected void handleRequest(HttpServletRequest request, HttpServletResponse response) throws TTransportException, IOException {
		response.addHeader("Content-Disposition", "attachment; filename=data.txt");
		IOUtils.copyLarge(CassandraFileSystem.getInstance(null).readFile(request.getRequestURI()), response.getOutputStream());
	}

}
