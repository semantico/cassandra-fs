package org.apache.cassandra.contrib.fs.site;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.contrib.fs.CassandraFileSystem;
import org.apache.commons.io.IOUtils;
import org.apache.thrift.transport.TTransportException;

public class GetFileView extends AbstractCassandraFsView{

	@Override
	protected void handleRequest(HttpServletRequest request, HttpServletResponse response) throws TTransportException, IOException {
		InputStream fileStream = CassandraFileSystem.getInstance(null).readFile(getPath(request));
		response.addHeader("Content-Disposition", "attachment; filename="+getFileName(request));
		IOUtils.copyLarge(fileStream, response.getOutputStream());
	}

}
