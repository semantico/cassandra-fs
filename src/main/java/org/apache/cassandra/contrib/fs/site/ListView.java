package org.apache.cassandra.contrib.fs.site;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.contrib.fs.CassandraFileSystem;
import org.apache.cassandra.contrib.fs.Path;
import org.apache.commons.lang.StringEscapeUtils;

public class ListView extends AbstractCassandraFsView {

	@Override
	protected void handleRequest(HttpServletRequest request,HttpServletResponse response) throws Exception {
		String dir = getPath(request);
		List<Path> subs = CassandraFileSystem.getInstance(null).list(dir);
		writePaths(response, dir, subs);
	}

	protected void writePaths(HttpServletResponse resp,String dir,List<Path> subs){
		resp.setContentType("text/xml");
		OutputStreamWriter writer = null;
		try {
			writer = new OutputStreamWriter(resp.getOutputStream());
			writer.write("<Paths dir=\"" + dir + "\">\n");
			for(Path sub : subs){
				writer.write("<Path>");
				writer.write(StringEscapeUtils.escapeXml(sub.getName()));
				writer.write("</Path>\n");	
			}
			writer.write("</Paths>");		
		}
		catch (IOException e) {
			e.printStackTrace();
		}finally {
			if(writer != null) {
				try {
					writer.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
