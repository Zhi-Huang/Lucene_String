package readpeer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import jboss.JClient;
import lucene.Analyzer;
import lucene.DataProcessor;
import lucene.Index;
import lucene.QueryConfig;
import lucene.ReturnValue;

public class Client {

	//a flag for debugging
	static boolean debug = true;
	
	static String[] ips;
	static int num_ip;
	static final int MAX_ip = 128;
	static JClient jclient;
	
	// name for each index
	static String passage = "Passage Index File";
	static String passage_content = "Passage Content Index File";
	static String highlight = "Hightlight Index File";
	static String comment = "Comment Index File";
	
	//specific application
	public static int PASSAGE_CONTENT = 5;
	public static int PASSAGE = 6;
	public static int HIGHLIGHT = 7;
	public static int COMMENT = 8;

	//for the analysis of the content
	public Analyzer analyzer;
	
	//Top K
	private int K = 1;
	
	public Client(String ipfile) throws Throwable {

		jclient = new JClient(ipfile);
		// connect all servers
		// content index is the default index
		jclient.connectAllServers(passage);
		
	}
	
	
	/**
	 * function for setting the index type
	 * */
	public void setIndexType(int type) {
		
		jclient.closeAllIndexwriters();
		try {
			if (type == PASSAGE)
				jclient.changeIndexfile(passage);
			else if (type == PASSAGE_CONTENT)
				jclient.changeIndexfile(passage_content);
			else if (type == HIGHLIGHT)
				jclient.changeIndexfile(highlight);
			else if (type == COMMENT)
				jclient.changeIndexfile(comment);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// initialization for building index
		jclient.initAllServers(Index.BUILD);
	}

	/**
	 * add the documents for random access
	 * */
	public void addDocument(int id, String doc) {

		if(debug) 
			System.out.println(id+"\t"+doc);
		
		strKey skey = new strKey(String.valueOf(id));
		strValue svalue = new strValue(doc);
		try {
			jclient.addDoc(skey, svalue);
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * add different kinds of data
	 * */
	public void addPairs(int id, String doc) {
		
		String grams[] = DataProcessor.getGrams(3, doc).split("\\|");
		for(int i = 0; i < grams.length; i++) {
			if(debug)
				System.out.println(id+"\t"+grams[i]);
			
			strKey skey = new strKey(String.valueOf(id));
			strValue svalue = new strValue(grams[i]);
			try {
				jclient.addPair(skey, svalue, Index.STRING_BUILD);
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * set the number of results we want
	 * */	
	public void setTopK (int k) {
		
		this.K = k;
	}
		
	/**
	 * test the insertion
	 * @throws IOException 
	 * */
	private void testInsertion() throws Throwable {
		
		// initialization for building index
		jclient.initAllServers(Index.BUILD);
		//read the data
		BufferedReader buf = new BufferedReader(new InputStreamReader(new FileInputStream("data/content.txt")));
		String line;
		int id = 0;
		while((line = buf.readLine()) != null) {
			this.addPairs(id, line);
			id++;
		}
		//we have to close the writer so that it can 
		jclient.closeAllIndexwriters();
		buf.close();
			
		//build another index for random access
		//initialize the server
		jclient.initAllServers(Index.BUILD, passage_content);
		//add documents
		buf = new BufferedReader(new InputStreamReader(new FileInputStream("data/content.txt")));
		id = 0;
		while((line = buf.readLine()) != null) {
			this.addDocument(id, line);
			id++;
		}
		//close the writer and write on the disk
		jclient.closeAllIndexwriters();
		buf.close();
	}
	
	/**
	 * test query
	 * @param String for query
	 * @throws Throwable 
	 * */
	private void testQuery(String qstr) throws Throwable {
		
		//initialize the query process
		jclient.initAllServers(Index.STRING_SEARCH, passage);	
		//set the query configurations
		//get grams
		String qgrams[] = DataProcessor.getGrams(3, qstr).split("\\|");
		int num = qgrams.length;
		//set attributes
		STRConfig configs[] = new STRConfig[num];
		for(int i = 0; i < num; i++) {
			if(debug)
				System.out.println("qgrams: "+qgrams[i]);
			configs[i] = new STRConfig(0, qgrams[i]);
		}
		ReturnValue revalue = jclient.answerQuery(configs);
		List<Map.Entry<String, Double[]>> list = revalue.sortedOncount();
		//display the result
		int result_num = Math.min(K, list.size()); 
		int indexs[] = new int[result_num];
		for(int i = 0; i < result_num; i++) {
			indexs[i] = Integer.valueOf(list.get(i).getKey());
			//for test
			if(debug) {
				System.out.println(indexs[i]+": "+jclient.getData(indexs[i], passage_content));	
			}
		}
	}
	
	
	public static void main(String[] agrs) throws Throwable {

		Client client = new Client("IPs");
//		client.testInsertion();
		client.setTopK(15);
		client.testQuery("drag onto the page ");
		client.jclient.disconnectAllServers();
	}
}

class STRConfig extends QueryConfig {

	public STRConfig(int i, String string) {
		// TODO Auto-generated constructor stub
		super(i, string);
	}

	@Override
	public double calcDistance(int a, int b) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return QueryConfig.STRING;
	}
	
}
