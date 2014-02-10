package jboss;

import javax.management.MBeanServer;

import lucene.Index;

import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

public class InitHandler implements ServerInvocationHandler{

	private Index index;
	
	public InitHandler(Index index) throws Throwable {
		
		this.index = index;
	}
	
	@Override
	public void addListener(InvokerCallbackHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object invoke(InvocationRequest arg0) throws Throwable {
		// TODO Auto-generated method stub
		int type = (Integer)arg0.getParameter();
		if(type == Index.BUILD)
			index.init_building();
		else if(type == Index.STRING_SEARCH)
			index.init_query();
		else if(type == Index.SIFT_SEARCH)
		{
			index.init_query();
			index.getEntrances();
		}
		else
			System.out.println("Initialization error");
		return null;
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
