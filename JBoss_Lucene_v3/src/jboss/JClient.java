package jboss;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kv.*;
import lucene.*;

import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;

import test.strValue;


/**
 * @author huang zhi 
 * The client build the index and handle the query 
 * */

public class JClient {

	private String[] locators;
	private InvokerLocator invokerlocator;
	private Vector<Client> machines;	
	//a value pool where the application can get value from 
	public Vector<ReturnValue> valuePool;
	public ReturnValue revalues[]; 
	
	/**
	 * construction method version 1
	 * */
	public JClient(String[] new_locator) throws Throwable {

		//initialization for JBoss
		if (new_locator != null)
			locators = new_locator;
		else {
			locators = new String[1];
			locators[0] = "socket://localhost:8888";
		}
		machines = new Vector <Client>();
		for(int i = 0;i < locators.length; i++) {
			if(locators[i] == null)
				break;
			invokerlocator = new InvokerLocator(locators[i]);
			machines.add(new Client(invokerlocator));
		}		
	}
	
	/**
	 * construction method version 2
	 * @throws Throwable 
	 * */
	public JClient (String ipfile) throws Throwable {
		
		this.setLocators(ipfile);
	}
	
	/**
	 * set the location of each server
	 * */
	private void setLocators(String ipfile)throws Throwable {
		
		machines = new Vector <Client>();
		BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream(ipfile)));
		String line = "";
		while ((line = buf.readLine()) != null) {
			invokerlocator = new InvokerLocator(line);
			machines.add(new Client(invokerlocator));
		}
	}

	/**
	 * change index
	 * */
	public void changeIndexfile (String index_file) throws Throwable {
		
		for(int i = 0; i < machines.size(); i++) {
			
			machines.elementAt(i).setSubsystem("Connect");
			machines.elementAt(i).invoke(index_file);
		}
	}
	
	/**
	 * connect all the Servers
	 * */
	public void connectAllServers(String index_file) {
		
		for(int i = 0; i < machines.size(); i++ ) {
			connectServer(i, index_file);
		}
	}
	
	/**
	 * connect specific server
	 * */
	public void connectServer(int id, String index_file) {
		
		try {
			machines.elementAt(id).setSubsystem("Connect");
			machines.elementAt(id).connect();
			machines.elementAt(id).invoke(index_file);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.err.println("Server: "+id+" Connection Failure.");
			e.printStackTrace();
			System.exit(0);
		}
		System.out.println("Server: "+id+" Successfully Connected!");
		
	}

	/**
	 * initialization for all servers
	 * */
	public void initAllServers(int type, String indexfile) {

		for(int i = 0; i < machines.size(); i++ ) {
			initServer(i, type, indexfile);
		}
	}
	
	public void initAllServers(int type) {

		for(int i = 0; i < machines.size(); i++ ) {
			initServer(i, type);
		}
	}
	
	/**
	 * initialization for specific server
	 * declare new index file
	 * */
	public void initServer(int id, int type, String indexfile) {
		
		try {
			machines.elementAt(id).setSubsystem("Connect");
			machines.elementAt(id).invoke(indexfile);
			machines.elementAt(id).setSubsystem("Init");
			machines.elementAt(id).invoke(type);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 *without declaring new index file
	 * */
	public void initServer(int id, int type) {
		
		try {
			machines.elementAt(id).setSubsystem("Init");
			machines.elementAt(id).invoke(type);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * add documents and build index for random access
	 * @throws Throwable 
	 * */
	public void addDoc(Key k, Value v) throws Throwable  {
		
		//distributed the documents on their id 
		int id = Integer.valueOf(k.toString()) % machines.size();
		machines.elementAt(id).setSubsystem("Add");
		KVPair pair = new KVPair(k, v);
		machines.elementAt(id).invoke(pair);
	}
	
	/**
	 * add kv pairs and build the index
	 * */
	public void addPair(Key k, Value v, int type) throws Throwable {
		
		int id = createServerID(machines.size(), v.getDocument());
		machines.elementAt(id).setSubsystem("Add");
		//need to format the data
		if(type == Index.SIFT_BUILD) {
			v.setDocument(DataProcessor.strFormat(v.getDocument()));
		}
		KVPair pair = new KVPair(k, v);
		machines.elementAt(id).invoke(pair);
	}
	
	/**
	 * distribute the data while building the index
	 * */
	private int createServerID(int total, String v) {
		
		return v.hashCode()%total;		
	}
	
	/**
	 * distribute the query tasks and search in parallel
	 * */
	public ReturnValue answerQuery(QueryConfig qconfigs[]) throws Throwable {
		
		ReturnValue result = new ReturnValue();
		//set sub system
		for(int i = 0; i < machines.size(); i++)
			machines.elementAt(i).setSubsystem("Query");
		//create a thread pool for queries
		ExecutorService executor = Executors.newCachedThreadPool();
		int threadnum = qconfigs.length;
		//create return value for each query configuration
		revalues = new ReturnValue[threadnum];
		Task tasks[] = new Task[threadnum]; 
		//lock for merging the tables
		Lock lock = new ReentrantLock();
		
		//distribute the task
		for(int i = 0; i < threadnum; i++) {
			revalues[i] = new ReturnValue();
			tasks[i] = new Task(machines, qconfigs[i], revalues[i]);
			//submit the query task 
			Future<ReturnValue> fuvalue = executor.submit(tasks[i]);
			//get return value
			revalues[i] = fuvalue.get();
			
			//handle the null pointer exception
			if(revalues[i] == null) {
				System.err.println("Null Return Value!");
				continue;
			}
			//merge the return result
			lock.lock();
			result.merge(revalues[i]);
			lock.unlock();
		}
		executor.shutdown();
		
		//for test part
		/*ReturnValue test = new ReturnValue();
		for(int i = 0; i < revalues.length; i++) {
			System.out.println(revalues[i].querystring);
			revalues[i].sortedOndis();
			test.merge(revalues[i]);
		}*/
		
		//wait for all the return value are returned
		while(isAllreturned(revalues) == false)
		{
			System.out.println("waiting");
			Thread.sleep(50);
		}
		
		return result;
	}
	
	/**
	 * get the data using the index
	 * */
	public String getData(int id, String index_file) {
		
		String data = "";
		//the input parameter for simple search
		String params = String.valueOf(id)+"|"+index_file;
		//get the server
		int server_id = id % machines.size();
		//set the sub-system of the server
		machines.elementAt(server_id).setSubsystem("SimpleSearch");
		try {
			//invoke the seraching function
			data = (String)machines.elementAt(server_id).invoke(params);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return data;
	}
	
	private boolean isAllreturned(ReturnValue revalues[]) {
		
		for(int i = 0; i < revalues.length; i++) {
			if(revalues[i].querystring == null)
				return false;
		}
		return true;
	}
	
	/**
	 * close the Index writer after building the index
	 * */
	public void closeIndexwriter(int id) throws Throwable {
		
		machines.elementAt(id).setSubsystem("CloseWriter");
		String message = (String) machines.elementAt(id).invoke(null);
		System.out.println(id+":"+message);
	}
	
	public void closeAllIndexwriters()  {
		
		for(int i = 0;i < machines.size(); i++)
			try {
				this.closeIndexwriter(i);
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	/**
	 * disconnect all the servers
	 * */
	public void disconnectAllServers() {
		
		for(int i = 0; i < machines.size(); i++ ) {
			disconnectServer(i);
		}
	}
	
	/**
	 * disconnect specific server
	 * */
	public void disconnectServer(int id){
		
		machines.elementAt(id).disconnect();
		System.out.println(id+" :Disconnected!");	
	}
	
}

/**
 * this class distributed tasks and get return values in parallel
 * */
class Task implements Callable<ReturnValue>  {
	
	private Vector<Client> machines;
	QueryConfig qconfig;
	ReturnValue revalue;
	
	Task(Vector<Client> machines, QueryConfig qconfig, ReturnValue revalue) {
		
		this.machines = machines;
		this.qconfig = qconfig;
		this.revalue = revalue;
	}

	@Override
	public ReturnValue call() throws Exception {
		// TODO Auto-generated method stub
		int machine_id = (qconfig.getQuerystring().hashCode() % machines.size());
		try {
			revalue = (ReturnValue) machines.elementAt(machine_id).invoke(qconfig);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			System.out.println("Errors in the distributed task");
			e.printStackTrace();
		}
		return revalue;
	}
}



