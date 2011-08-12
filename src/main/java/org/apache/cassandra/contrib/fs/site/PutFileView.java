package org.apache.cassandra.contrib.fs.site;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.contrib.fs.CassandraFileSystem;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

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
