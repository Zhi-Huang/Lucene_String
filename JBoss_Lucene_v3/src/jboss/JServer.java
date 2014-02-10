package jboss;

import java.net.MalformedURLException;

import javax.management.MBeanServer;

import lucene.Index;

import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.transport.Connector;

public class JServer {

	//default locator
	private String locator = "socket://localhost:8888";
	private Connector connector;
	//default index file name
	private String indexfile;
	
	public JServer() {

	}

	public JServer(String new_locator) {
		
		this.locator = new_locator;
	}
	
	public void setLocator(String new_locator) {
		
		this.locator = new_locator;
	}

	public void setIndexFilename(String filename) {
	
		this.indexfile = filename;
	}
	
	public void init() throws Throwable {
		
		InvokerLocator myLocator = new InvokerLocator(locator);
		connector = new Connector();
		connector.setInvokerLocator(myLocator.getLocatorURI());
		connector.create();
		//create a index for Top-k Search
		Index index = new Index();
		//create a index for simple search
		Index simple_index = new Index();
		//add handlers for invocations
		connector.addInvocationHandler("Add", new AddDocHandler(index));
		connector.addInvocationHandler("Query", new QueryHandler(index));
		connector.addInvocationHandler("Connect", new ConnectHandler(index));
		connector.addInvocationHandler("CloseWriter", new CloseWriterHandler(index));
		connector.addInvocationHandler("Init", new InitHandler(index));
		//add a handler for simple search 
		connector.addInvocationHandler("SimpleSearch", new SimpleSearchHandler(simple_index));
		
		System.out.println("The Server is initialized.");
	}

	public void start() throws Throwable {
		connector.start();
		System.out.println("The server is on...");
	}

	public void stop() {
		connector.stop();
		System.out.println("The server is off.");
	}

}