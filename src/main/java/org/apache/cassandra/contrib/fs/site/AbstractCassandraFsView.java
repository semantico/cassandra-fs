package org.apache.cassandra.contrib.fs.site;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
		} finally {
			response.getOutputStream().flush();
		}
	}
	
	abstract protected void handleRequest(HttpServletRequest request, HttpServletResponse response) throws Exception;
	
	protected void writeSuccessful(HttpServletResponse resp,String msg) {
		resp.setContentType("text/xml");
		OutputStreamWriter writer = null;
		try {
			//TODO: use more advanced xml generation
			writer = new OutputStreamWriter(resp.getOutputStream());
			writer.write("<Response>\n");
			writer.write("<Code>\n");
			writer.write("OK");
			writer.write("</Code>\n");
			writer.write("<Message>\n");
			writer.write(msg); //TODO: replace special chars
			writer.write("</Message>\n");
			writer.write("</Response>");
		}
		catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(writer != null) {
				try {
					writer.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	protected void writeException(HttpServletResponse resp, CfsSiteException ex) {
		resp.setContentType("text/xml");
		OutputStreamWriter writer = null;
		try {
			writer = new OutputStreamWriter(resp.getOutputStream());
			writer.write("<Error>\n");
			writer.write("<Code>\n");
			writer.write(StringEscapeUtils.escapeXml(ex.getErrorCode()));
			writer.write("</Code>\n");
			writer.write("<Message>\n");
			writer.write(StringEscapeUtils.escapeXml(ex.getMessage())); 
			writer.write("</Message>\n");
			writer.write("</Error>");
		}
		catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(writer != null) {
				try {
					writer.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
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

	protected String getPath(HttpServletRequest request){
		return request.getRequestURI();
	}
	
	protected String getFileName(HttpServletRequest request) {
		String[] parts = request.getRequestURI().split("/");
		return parts[parts.length-1];
	}
	
}
