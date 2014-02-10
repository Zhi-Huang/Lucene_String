package lucene;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Vector;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;

public abstract class QueryConfig implements Serializable{

	public int queryId;
	//whether to scan from the initial point of the list 
	public boolean needRestart;
	//store the position in previous scan
	public int pre_position;
	
	//the type of application
	public static int STRING = 0;
	public static int SIFT = 1;

	//for sift feature
	public int dim;
	public int dim_value;
	
	//for general search
	private String querystr;
	
	//the range for di-direction search, @deprecated now
	private int up;
	private int low;
	
	//the length of scanning the list of each key 
	private int scanlength;

	//abstract functions
	//calculate the distance
	public abstract double calcDistance(int a, int b);
	
	//return the search type
	public abstract int getType();
	
	public QueryConfig () {
		
		//need to restart the scan process by default 
		this.needRestart = true;
		this.scanlength = Integer.MAX_VALUE;
	}
	
	/**
	 * construction function for string query
	 * */
	public QueryConfig (int id, String qstring) {
			
		//need to restart the scan process by default 
		this.needRestart = true;
		this.queryId = id;
		this.querystr = qstring;
		this.scanlength = Integer.MAX_VALUE;
	}
	
	public QueryConfig (int id, String qstring, int scanlength) {
			
		//need to restart the scan process by default 
		this.needRestart = true;
		this.queryId = id;
		this.querystr = qstring;
		this.scanlength = scanlength;
	}
	
	/**
	 * construction function for sift query
	 * */
	public QueryConfig (int id, String qstring, int length, int up, int low) {
		
		//need to restart the scan process by default 
		this.needRestart = true;
		this.queryId = id;
		this.querystr = qstring;
		this.scanlength = length;
		this.up = up;
		this.low = low;
	}
	
	/**
	 * set the bi-direction search range
	 * @deprecated
	 * */
	public void setRange(int low, int up) {

		this.low = low;
		this.up = up;
	}

	/**
	 * @deprecated
	 * */
	public int getUpRange() {

		return this.up;
	}

	/**
	 * @deprecated
	 * */
	public int getLowRange() {

		return this.low;
	}

	public void setQuerystring(String str) {

		this.querystr = str;
	}

	public String getQuerystring() {

		return this.querystr;
	}

	public void setLength(int length) {
		
		this.scanlength = length;
	}
	
	public int getLength() {
		
		return this.scanlength;
	}
	
}
