package org.apache.cassandra.contrib.fs.site;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.contrib.fs.Path;
import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.web.servlet.view.AbstractView;

public abstract class AbstractCassandraFsView extends AbstractView {

	@Override
	protected void renderMergedOutputModel(Map<String, Object> model,HttpServletRequest request, HttpServletResponse response)throws Exception {
		try {
			handleRequest(request, response);
		} catch (Exception e) {
			writeException(response, new CfsSiteException(e.getClass().getName(),e.getMessage()));
		}
	}
	
	abstract protected void handleRequest(HttpServletRequest request, HttpServletResponse response) throws Exception;
	
	protected void writeSuccessful(HttpServletResponse resp,String msg){
		resp.setContentType("text/xml");
		try {
			//TODO: use more advanced xml generation
			resp.getWriter().write("<Response>\n");
				resp.getWriter().write("<Code>\n");
					resp.getWriter().write("OK");
				resp.getWriter().write("</Code>\n");
				
				resp.getWriter().write("<Message>\n");
					resp.getWriter().write(msg); //TODO: replace special chars
				resp.getWriter().write("</Message>\n");
			resp.getWriter().write("</Response>");	
		}
		catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	protected void writeException(HttpServletResponse resp, CfsSiteException ex) {
		resp.setContentType("text/xml");
		try {
			OutputStream out = resp.getOutputStream();
			out.write("<Error>\n".getBytes());
			out.write("<Code>\n".getBytes());
			out.write(StringEscapeUtils.escapeXml(ex.getErrorCode()).getBytes());
			out.write("</Code>\n".getBytes());
			out.write("<Message>\n".getBytes());
			out.write(StringEscapeUtils.escapeXml(ex.getMessage()).getBytes()); 
			out.write("</Message>\n".getBytes());
			out.write("</Error>".getBytes());
			out.flush();
		}
		catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	protected void writePaths(HttpServletResponse resp,String dir,List<Path> subs){
		resp.setContentType("text/xml");
		try {
			resp.getWriter().write("<Paths dir=\"" + dir + "\">\n");
			for(Path sub : subs){
				resp.getWriter().write("<Path>");
					resp.getWriter().write(StringEscapeUtils.escapeXml(sub.getName()));
				resp.getWriter().write("</Path>\n");	
			}
			resp.getWriter().write("</Paths>");		
		}
		catch (IOException e) {
			e.printStackTrace();
		}	
	}

	protected String getPath(HttpServletRequest request){
		return request.getRequestURI();
	}
	
}
