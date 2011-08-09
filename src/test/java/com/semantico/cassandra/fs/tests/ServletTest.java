package com.semantico.cassandra.fs.tests;

import static org.junit.Assert.*;

import java.io.IOException;

import javax.servlet.ServletException;
import org.apache.cassandra.contrib.fs.site.SimpleCfsServlet;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.junit.Before;
import org.junit.Test;

public class ServletTest extends AbstractCassandraFsTest {
	
	private SimpleCfsServlet servlet;
	private MockHttpServletRequest mockRequest;
	private MockHttpServletResponse mockResponse;
	
	@Before
	public void setUpServlet() {
		servlet = new SimpleCfsServlet();
		mockRequest = new MockHttpServletRequest();
		mockRequest.setContent(new byte[0]);
		mockResponse = new MockHttpServletResponse();
	}
	
	@Test
	public void testBlankDoGet() throws ServletException, IOException {
		servlet.doGet(mockRequest, mockResponse); //should fail silently
	}
	
	@Test
	public void testBlankDoPut() throws ServletException, IOException {
		servlet.doPut(mockRequest, mockResponse); //should fail silently
	}
	
	@Test
	public void testBlankDoDelete() throws ServletException, IOException {
		servlet.doDelete(mockRequest, mockResponse); //should fail silently
	}
	
	@Test
	public void testDoGetList1() throws ServletException, IOException {
		mockRequest.setRequestURI("/");
		mockRequest.setParameter("action", "LS");
		servlet.doGet(mockRequest, mockResponse);
		String responseContent = mockResponse.getContentAsString();
		assertNotNull(responseContent);
		assertTrue(responseContent.length() > 0);
		assertEquals("<Paths dir=\"/\">\n</Paths>", responseContent);
	}
	
	
	
	@Test
	public void testDoPutFile() throws ServletException, IOException {
		mockRequest.setRequestURI("/newFolder/testFile.txt");
		mockRequest.setContent("someText".getBytes());
		servlet.doPut(mockRequest, mockResponse);
		String responseContent = mockResponse.getContentAsString();
		assertEquals("<Response>\n<Code>\nOK</Code>\n<Message>\n/newFolder/testFile.txt</Message>\n</Response>", responseContent);
	}
	
	@Test
	public void testDoGetFile() throws ServletException, IOException {
		testDoPutFile();
		mockResponse = new MockHttpServletResponse();
		servlet.doGet(mockRequest, mockResponse);
		String responseContent = mockResponse.getContentAsString();
		assertEquals("someText", responseContent);
	}
	
	@Test
	public void testDoDeleteFile() throws ServletException, IOException {
		testDoGetFile();
		mockResponse = new MockHttpServletResponse();
		servlet.doDelete(mockRequest, mockResponse);
		String responseContent = mockResponse.getContentAsString();
		assertEquals("<Response>\n<Code>\nOK</Code>\n<Message>\n/newFolder/testFile.txt</Message>\n</Response>", responseContent);
	}
	
	@Test
	public void testFileWasDeleted() throws ServletException, IOException {
		testDoDeleteFile(); //puts the file, gets the file, deletes the file. now get the file again to ensure it isnt there anymore
		mockResponse = new MockHttpServletResponse();
		servlet.doGet(mockRequest, mockResponse);
		String responseContent = mockResponse.getContentAsString();
		assertEquals("<Error>\n<Code>\nIOException</Code>\n<Message>\nNotFoundException()</Message>\n</Error>", responseContent);
	}
	
	@Test
	public void testDoGetList2() throws ServletException, IOException {
		testDoGetFile();
		setUpServlet();
		mockRequest.setRequestURI("/newFolder/");
		mockRequest.setParameter("action", "LS");
		servlet.doGet(mockRequest, mockResponse);
		String responseContent = mockResponse.getContentAsString();
		assertEquals("<Paths dir=\"/newFolder/\">\n<Path>testFile.txt</Path>\n</Paths>", responseContent);
	}
}
