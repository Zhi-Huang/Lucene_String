package jboss;

import javax.management.MBeanServer;

import kv.*;

import lucene.Index;
import lucene.QueryConfig;

import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

public class SimpleSearchHandler implements ServerInvocationHandler {

	private Index index;	
	
	SimpleSearchHandler (Index index) throws Throwable {
		
		this.index = index;
	}
	
	@Override
	public void addListener(InvokerCallbackHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object invoke(InvocationRequest arg) throws Throwable {

		// TODO Auto-generated method stub
		//the input parameter contains two parts: "id|filename"
		String parameters = (String)arg.getParameter();
		String parameter[] = parameters.split("\\|");
		//init the search 
		index.initSimpleSearch(parameter[1]);
		//search
		return index.simpleSearch(parameter[0]);
	}

	@Override
	public void removeListener(InvokerCallbackHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setInvoker(ServerInvoker arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setMBeanServer(MBeanServer arg0) {
		// TODO Auto-generated method stub
		
	}

}
