/**
 * @author huang zhi
 * handle the invocation for adding the doc to index
 * */

package jboss;

import lucene.Index;

import javax.management.MBeanServer;

import kv.*;

import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

public class AddDocHandler implements ServerInvocationHandler {

	private Index index;
	
	AddDocHandler(Index index) throws Throwable{
		
		this.index = index;
	}
	
	@Override
	public void addListener(InvokerCallbackHandler arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	/**
	 * Use this method to add the Key-Value pairs into the index
	 * */
	public Object invoke(InvocationRequest arg0) throws Throwable {
		// TODO Auto-generated method stub
		Object object = arg0.getParameter();
		KVPair pair = (KVPair)object;
		index.addDoc(pair.key, pair.value);
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
