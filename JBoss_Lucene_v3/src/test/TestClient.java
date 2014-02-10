package test;

import java.io.*;

import jboss.*;
import lucene.*;

import java.lang.*;
import java.util.*;

/**
 * this is a class for testing the functions for building and searching
 * */

public class TestClient {
	
	static String[] ips;
	static int num_ip;
	static final int MAX_ip = 100;
	static JClient jclient;
	
	public static void main(String [] args) throws Throwable {
		
		connectServers("IPs", "Test String Index File");
//		BuildIndex("test_string",Index.STRING_BUILD);
		stringQuery();
		jclient.disconnectServer(0);
//		connectServers("IPs", "Test Sift Index File");	
//		BuildIndex("test_sift",Index.SIFT_BUILD);
//		BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream("sift_query")));
//		siftQuery(buf.readLine());
	}
	
	/**
	 * connect the servers
	 * the location of server is stored in the input file
	 * */
	static void connectServers(String inputfile, String indexfile) throws Throwable {
		BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream(inputfile)));
		String line = "";
		ips = new String[MAX_ip];
		num_ip = 0;
		while((line = buf.readLine()) != null)	{
			ips[num_ip] = line;
			num_ip++;
		}
		//connect servers
		jclient = new JClient(ips);
		for(int i = 0;i < num_ip; i++)
			jclient.connectServer(i, indexfile);
		buf.close();
	}
	
	
	/**
	 * building the index for strings and sift
	 * @throws Throwable 
	 * */
	static void BuildIndex(String datafile, int mode) throws Throwable {
		
		//init the index
		for(int i = 0;i < num_ip; i++)
			jclient.initServer(i, Index.BUILD);
		
		BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream(datafile)));
		String line;	
		int id = 0;
		while ((line = buf.readLine()) != null) {
			//for insertion the key is elementID
			strKey strkey = new strKey(String.valueOf(id));
			String values[] = null;
			if(mode == Index.STRING_BUILD)
				values = DataProcessor.process(2, line).split(" ");
			else if(mode == Index.SIFT_BUILD)
				values = line.split(" ");
			else
				System.out.println("Type error");
			
			for (int i = 0; i < values.length; i++) {
				//for insertion the value is grams and dim+value
				strValue strvalue = null;
				if(mode == Index.SIFT_BUILD)
					strvalue = new strValue(i+"+"+values[i]);
				else if(mode == Index.STRING_BUILD)
					strvalue = new strValue(values[i]);
				else
					System.out.println("Type error when adding document");
				jclient.addPair(strkey, strvalue, mode);
			}
			id++;
		}	
		for(int i = 0;i < num_ip; i++)
			jclient.closeIndexwriter(i);
	}
	
	
	/**
	 * test string query
	 * @throws Throwable 
	 * */
	static void stringQuery() throws Throwable {
		
		//initialize some data on the server for string search
		for(int i = 0;i < num_ip; i++)
			jclient.initServer(i, Index.STRING_SEARCH);
		
		int length = 2;
		Stringconfig[] configs = new Stringconfig[length];
		for(int i = 0; i < length; i++) {
			configs[i] = new Stringconfig();
			//set the query id
			configs[i].queryId = i;
		}
		//set the query string
		configs[0].setQuerystring("co");
		//set the length to scan the value list
		configs[0].setLength(5);
		configs[1].setQuerystring("ph");
		configs[1].setLength(5);

		//scan for first time
		ReturnValue revalues[] = new ReturnValue[1];
		revalues[0].merge(jclient.answerQuery(configs));
		//display
		for (int i = 0; i < revalues.length; i++) {
			System.out.println(revalues[i].querystring);
			revalues[i].sortedOncount();
		}
		
		//if we want to continue to scan the list 
		//set the signal and call the function again
		configs[0].needRestart = false;
		configs[1].needRestart = false;
		
		//send the query again 
		revalues[0] = jclient.answerQuery(configs);
		//display
		for (int i = 0; i < revalues.length; i++) {
			System.out.println(revalues[i].querystring);
			revalues[i].sortedOncount();
		}
	}
	
	/**
	 * test the query of sift feature
	 * @param query is a string of values
	 * @throws Throwable 
	 * */
	static void siftQuery(String query) throws Throwable {
		
		//initialize the server
		for(int i = 0;i < num_ip; i++)
			jclient.initServer(i, Index.SIFT_SEARCH);
		
		
		String values[] = query.split(" ");
		int length = 128;
		Siftconfig siftconfigs[] = new Siftconfig[length];
		for(int i = 0; i < length; i++) {
			//set the configurations for query
			siftconfigs[i] = new Siftconfig();
			siftconfigs[i].dim = i;
			siftconfigs[i].dim_value = Integer.valueOf(values[i].trim());
			siftconfigs[i].setQuerystring(String.valueOf(i)+"+"+values[i]);
			siftconfigs[i].setRange(siftconfigs[i].dim_value - 1, siftconfigs[i].dim_value + 2);
			//scan length for value list
			siftconfigs[i].setLength(3);
		}
		//get return value for each query configuration
		ReturnValue revalues[] = new ReturnValue[1];
		revalues[0] = jclient.answerQuery(siftconfigs);
		
		for (int i = 0; i < revalues.length; i++) {
			System.out.println(revalues[i].querystring+":");
			revalues[i].sortedOncount();
			revalues[i].sortedOndis();
		}
		
	}
	
	
}

/**
 * QueryConfig is an abstract class 
 * You should extends the class to specify some functions
 * */
class Stringconfig extends QueryConfig implements Serializable {

	Stringconfig() {} 
	
	Stringconfig (String string) {
		this.setQuerystring(string);
	}
	
	
	@Override
	public double calcDistance(int a, int b) {
		// TODO Auto-generated method stub
		return (a - b) * (a - b);
	}

	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return QueryConfig.STRING;
	
	}		
}

/**
 *  For sift feature query 
 * */
class Siftconfig extends QueryConfig implements Serializable  {
	
	public Siftconfig(){}
	
	public Siftconfig(int id, String qstring, int length, int up, int low) {
		// TODO Auto-generated constructor stub
		super(id,qstring,length,up,low);
	}

	public void setQuerystring(String string) {
		//for sift feature, the query string should in certain format
		super.setQuerystring(DataProcessor.strFormat(string));
	}
	
	@Override
	public double calcDistance(int a, int b) {
		// TODO Auto-generated method stub
		return (a - b) * (a - b);
	}

	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return QueryConfig.SIFT;
	}
	
}