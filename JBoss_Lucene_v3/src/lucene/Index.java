package lucene;


import java.io.*;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.net.*;

import kv.Key;
import kv.Value;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;



/**
 * building the inverted with the interface of lucene
 * scanning the inverted list
 * @author huang zhi
 * */

public class Index {
	
	//for debug
	boolean debug = true;
	
	//for general use
	public static int BUILD = 0;
	public static int SIFT_BUILD = 1;
	public static int STRING_BUILD = 2;
	public static int STRING_SEARCH = 3;
	public static int SIFT_SEARCH = 4;
	
	private File indexFile;
	private Analyzer analyzer;
	private MMapDirectory MMapDir;
	private IndexWriterConfig config;
	private IndexWriter MMwriter;
	private IndexReader indexReader;
	private AtomicReader areader;
	private Vector<DocsEnum> valuelist_entrances;
	private String fieldname1 = "ElementID";
	private String fieldname2 = "ElementValue";
	private HashMap<String, Integer> index_map;
	//store all the keys
	private Vector<String> keylist;
	//store the search position of value list enumerator
	private HashMap<String, DocsEnum> position_map;
	//store the search position of value list enumerator for bi-direction search
	private HashMap<String, DocsEnum[]> bi_position_map;
	//store the index of value list entrances for expandSearch
	private HashMap<String, Integer[]> bi_index_map;
	
	public Index() {}
	
	public void setIndexfile (String indexfilename) {
		
		this.indexFile = new File(indexfilename);
		System.out.println("The Index File is setted: "+indexfilename);
	}
	
	/**
	 * initialization for building the index 
	 * @throws Throwable 
	 * */
	public void init_building() throws Throwable {
		//change the default stop words list of standard analyzer
		//otherwise it may flit some of the grams
		char[] new_stop_words = new char[1];
		new_stop_words[0] = 'a';
		analyzer = new StandardAnalyzer(Version.LUCENE_45, new CharArrayReader(new_stop_words));
		//MMap
		MMapDir = new MMapDirectory(indexFile);
		//set the configuration of index writer
		config = new IndexWriterConfig(Version.LUCENE_45,analyzer);
		//use Memory Map to store the index
		MMwriter = new IndexWriter(MMapDir,config);
	}
	
	/**
	 * build the inverted list and store it in a file
	 * */ 
	public void addDoc(Key k, Value v){
		Document doc = new Document();
		doc.add(new TextField(fieldname1, k.toString(), Field.Store.YES));
		//no analysis on field2
		doc.add(new StringField(fieldname2, v.getDocument(), Field.Store.YES));
		try {
			MMwriter.addDocument(doc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("index writer error");
			e.printStackTrace();
		}
	} 
	
	/**
	 * initialize the query process
	 * */
	public void init_query() throws Throwable {
		
		indexReader = DirectoryReader.open(MMapDirectory.open(indexFile));
		areader = SlowCompositeReaderWrapper.wrap(indexReader);
		position_map = new HashMap<String,DocsEnum>();
		bi_position_map = new HashMap<String, DocsEnum[]>();
		bi_index_map = new HashMap<String, Integer[]>();
	}
	
	/**
	 * store all the iterators for each key into a vector
	 * so that we can randomly visit the list
	 * @return Vector<DocsEnum>
	 * */
	public void getEntrances() throws Throwable {
		
		Terms terms = areader.terms(fieldname2);
		TermsEnum te = null;
		te = terms.iterator(te);
		//for storage
		Vector<DocsEnum> iterators = new Vector<DocsEnum>();
		//this is a map for locating the query key in the vector
		index_map = new HashMap<String, Integer>();
		//this vector stores all the keys and it is used for get search range in value list entrances
		keylist = new Vector<String>();
		String keystring;
		int index = 0;
		while(te.next() != null) {
			keystring = te.term().utf8ToString();
			DocsEnum enumer  = areader.termDocsEnum(new Term(fieldname2,keystring));
			//store the index of key strings
			index_map.put(keystring, Integer.valueOf(index));
			index++;
			//store all the keys
			keylist.add(keystring);
			//store the entrances
			iterators.add(enumer);
		}
		valuelist_entrances =  iterators;
	}

	/**
	 * search for different kinds of data
	 * @return ReturnValue
	 * @throws Throwable 
	 * */
	public ReturnValue generalSearch(QueryConfig config) throws Throwable {
		
		if (config.getType() == QueryConfig.STRING)
			return stringSearch(config);
		else if(config.getType() == QueryConfig.SIFT)
			return siftSearch(config);
		else
		{
			System.err.println("Search type error");
			return null;
		}
	}
	
	public ReturnValue stringSearch (QueryConfig config) throws Throwable {
		
		ReturnValue revalue = new ReturnValue();
		//store the query
		revalue.querystring = config.getQuerystring();
		//the enumerator of value list
		DocsEnum value_enum;
		//create a term for query
		Term qterm = new Term(fieldname2,config.getQuerystring());
		//store the query id and query string
		String id_query = String.valueOf(config.queryId)+"+"+config.getQuerystring();
		//if we do not have to scan from the beginning
		if(config.needRestart == false) {
			if(position_map.containsKey(id_query)) {
				System.out.println(id_query + " is found in map");
				//get the enumerator directly from the hash map
				value_enum = position_map.get(id_query);
			}
			else
			{
				System.out.println("preposition: "+config.pre_position);
				value_enum = areader.termDocsEnum(qterm);
				//scan to the previous position
				value_enum.advance(config.pre_position);
			}
		}
		//else create new enumerator and scan from beginning
		else {
			//find the exact entrance of the value list
			value_enum = areader.termDocsEnum(qterm);
		}
		//scan the list for a certain length
		int count = 0, lucene_doc_id = -1;
		String element_id = null;
		while (count < config.getLength()) {
			//not found
			if(value_enum == null) {
				System.out.println("the feature is not found");
				return null;
			}
			//scan the list
			lucene_doc_id = value_enum.nextDoc();
			//at the end of the list
			if(lucene_doc_id == DocsEnum.NO_MORE_DOCS) {
				revalue.current_values.add("-1");
				//store current position
				revalue.pre_position = lucene_doc_id;
				position_map.put(id_query, value_enum);
				return revalue;
			}
			count++;
			
			//update the table
			//get the element id
			element_id = indexReader.document(lucene_doc_id).getField(fieldname1).stringValue();
			//update count
			Double count_distance[] = new Double[2];
			if(revalue.table.containsKey(element_id)) {
				count_distance = revalue.table.get(element_id);
				//count++
				count_distance[0] += 1.0;
				revalue.table.put(element_id, count_distance);
			}
			else {
				//count = 1
				count_distance[0] = 1.0;
				//accumulate distance = 0
				count_distance[1] = 0.0;
				revalue.table.put(element_id, count_distance);
			}
		}
		revalue.current_values.add(element_id);
		//store the current enumerator
		revalue.pre_position = lucene_doc_id;
		//update the position map
		position_map.put(id_query, value_enum);
		return revalue;
	}
	
	/**
	 * sift search strategy one
	 * */
	
	public ReturnValue siftSearch(QueryConfig config) throws Throwable {
		
		ReturnValue revalue = new ReturnValue();
		//store the query
		revalue.querystring = config.getQuerystring();
		//if this is the first time searching we have to do point search first
		if(config.needRestart == true)
		{
			//get value list entrance
			int entrance_index = index_map.get(config.getQuerystring());
			DocsEnum point_enum = valuelist_entrances.elementAt(entrance_index);
			int lucene_doc_id = -1;
			Double count_dis[] = new Double[2];
			//scan the whole list
			while((lucene_doc_id = point_enum.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
				//parse the document
				String element_id = indexReader.document(lucene_doc_id).getField(fieldname1).stringValue();
				String sift_feature = indexReader.document(lucene_doc_id).getField(fieldname2).stringValue();
				int sift_value = DataProcessor.getSiftValue(sift_feature);
				//update the table
				count_dis[0] = 1.0;	
				count_dis[1] = config.calcDistance(config.dim_value, sift_value);
				revalue.table.put(element_id, count_dis);
			}
			//expand search in serial, maybe we can change it into parallel
			//expand upwards
			HashMap<String, Double[]> search_result = 
				expandSearch(
						//the entrance index of the start searching point 
						entrance_index - 1,
						//the enumerator of expand list 
						valuelist_entrances.elementAt(entrance_index - 1),
						//scan length
						config.getLength(),
						//expand direction: -1 for up, +1 for down
						-1,
						//get the distance function
						config
						);
			revalue.table.putAll(search_result);
			search_result.clear();
			//expand downwards
			search_result = 
				expandSearch(
						//the entrance index of the start searching point 
						entrance_index + 1,
						//the enumerator of expand list 
						valuelist_entrances.elementAt(entrance_index + 1),
						//scan length
						config.getLength(),
						//expand direction: -1 for up, +1 for down
						+1,
						//get the distance function
						config
						);
			revalue.table.putAll(search_result);
		}
		
		return revalue;
	}
	
	/**
	 * Recursively expand the searching range
	 * @throws Throwable 
	 * */
	private HashMap<String, Double[]> expandSearch(
			int start_index, DocsEnum docenum, int num, int direction, QueryConfig q) throws Throwable {
		
		HashMap<String, Double[]> expandmap = new HashMap<String, Double[]>();
		//keys for the storage of position
		String pos_key = String.valueOf(q.queryId)+"+"+q.getQuerystring();
		//start_indexs[0] enums[0] store position of the upwards expansion
		//start_indexs[1] enums[1] store position of the downwards expansion
		DocsEnum enums[] = new DocsEnum[2];
		Integer start_indexs[] = new Integer[2];

		int lucene_doc_id = -1;
		Double count_dis[] = new Double[2];
		int sift_value = -1;
		//scan current list
		while(num > 0 && (lucene_doc_id = docenum.nextDoc())!= DocsEnum.NO_MORE_DOCS) {
			num--;
			//update the table
			String element_id = indexReader.document(lucene_doc_id).getField(fieldname1).stringValue();
			String sift_feature = indexReader.document(lucene_doc_id).getField(fieldname2).stringValue();
			sift_value = DataProcessor.getSiftValue(sift_feature);
			count_dis[0] = 1.0;
			count_dis[1] = q.calcDistance(sift_value, q.dim_value);
			expandmap.put(element_id, count_dis);
		}
		//if get enough element, no expansion 
		if(num == 0) {
			//store the current position
			if(bi_position_map.containsKey(pos_key)) {
				enums = bi_position_map.get(pos_key);
				start_indexs = bi_index_map.get(pos_key);
			}
			//upwards
			if(direction == -1) {
				enums[0] = docenum;
				start_indexs[0] = start_index;
			}
			//downwards
			else {
				enums[1] = docenum;
				start_indexs[1] = start_index;
			}
			bi_position_map.put(pos_key, enums);
			bi_index_map.put(pos_key,start_indexs);
			return expandmap;
		}
			
		//else we have to consider whether to expand
		else {
			//if we already reach the bound, no expansion 
			if ((sift_value == 0 && direction == -1) || (sift_value == 255 && direction == 1)) {
				//store the current position
				if(bi_position_map.containsKey(pos_key)) {
					enums = bi_position_map.get(pos_key);
					start_indexs = bi_index_map.get(pos_key);
				}
				//upwards
				if(direction == -1) {
					enums[0] = docenum;
					start_indexs[0] = start_index;
				}
				//downwards
				else {
					enums[1] = docenum;
					start_indexs[1] = start_index;
				}
				bi_position_map.put(pos_key, enums);
				bi_index_map.put(pos_key,start_indexs);
				return expandmap;
			}
			//else expand
			else {
				int new_index = start_index + direction;
				DocsEnum new_enum = valuelist_entrances.elementAt(new_index);
				//recursively expand
				HashMap<String, Double[]> new_expand_map = expandSearch(new_index, new_enum, num, direction, q);
				//just simply put the hash map together will be fine
				//because each element_id could only appear once in one dimension
				expandmap.putAll(new_expand_map);
				return expandmap;
			}
		}
	}
	
	/**
	 * sift search strategy two
	 * */
	public ReturnValue siftSearch_2 (QueryConfig config) throws Throwable {
		
		ReturnValue revalue = new ReturnValue();
		//store the query
		revalue.querystring = config.getQuerystring();
		//get the search range
		int search_range[] = getRange(config);
		//search from low bound to up bound
		for(int i = search_range[0]; i <= search_range[1]; i++) {
			
			Double count_distance[] = new Double[2];
			//scan the list and update the table
			DocsEnum docenum;
			String id_key = String.valueOf(config.queryId) + "+" +keylist.elementAt(i);
			//if we need to scan from beginning
			if(config.needRestart == true)
				docenum = valuelist_entrances.elementAt(i);
			//otherwise
			else {
				//if the key is recorded in the hashmap then get it
				if(position_map.containsKey(id_key)) {
					docenum = position_map.get(id_key);
				}
				//if the key cannot be found in the hashmap then scan to found right position
				else {
					docenum = valuelist_entrances.elementAt(i);
					//scan to the right position
					docenum.advance(config.pre_position);
				}
			}
			
			int lucene_doc_id, count = 0, scanlength = 0;
			scanlength = config.getLength();
			while(count < scanlength) {
				count++;
				lucene_doc_id = docenum.nextDoc();
				//to the end of list
				if(lucene_doc_id == DocsEnum.NO_MORE_DOCS) {
					revalue.pre_position = lucene_doc_id;
					position_map.put(id_key, docenum);
					break;
				}
				//parse the document
				String element_id = indexReader.document(lucene_doc_id).getField(fieldname1).stringValue();
				String sift_feature = indexReader.document(lucene_doc_id).getField(fieldname2).stringValue();
				int sift_value = DataProcessor.getSiftValue(sift_feature);
				//update the table
				count_distance[0] = 1.0;	
				count_distance[1] = config.calcDistance(config.dim_value, sift_value);
				System.out.println("id: "+element_id+" count:"+count_distance[0]+" dis: "+count_distance[1]);
				revalue.table.put(element_id, count_distance);
			}
			//store current position of enumerator
			position_map.put(id_key, docenum);
		}
		System.out.println("/////////////////////////");
		return revalue;
	}	
	
	/**
	 * map the bound to the index in entrances vector
	 * range[0]: low bound index
	 * range[1]: up bound index
	 * */
	private int[] getRange(QueryConfig config) {
		
		int[] ranges = new int[2];
		//Implementation
		int vec_index = index_map.get(config.getQuerystring());
 
		int i = vec_index;
		int dim = config.dim;
		
		//get low bound index in vector
		while(i > 0 && DataProcessor.getSiftDim(keylist.elementAt(i)) == dim 
				&& DataProcessor.getSiftValue(keylist.elementAt(i)) > config.getLowRange())
			i--;
		ranges[0] = i;
		
		//get up bound index in vector
		i = vec_index;
		while(DataProcessor.getSiftDim(keylist.elementAt(i)) == dim 
				&& DataProcessor.getSiftValue(keylist.elementAt(i)) < config.getUpRange()
				&& i < keylist.size())
			i++;
		ranges[1] = i;
		return ranges;
	}
	
	
	/**
	 * preparing for simple search
	 * */
	public void initSimpleSearch(String index_file) {
		
		//if the index is already initialized then we do not have to do it again
		if(indexFile != null && indexFile.getName().equals(index_file) && areader != null) {
			if(debug){
				System.out.println("index file is already initialized: "+index_file);
			}
		}
		else{
			//set new index file and initialize the query process
			setIndexfile(index_file);
			try {
				init_query();
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * for random access
	 * */
	public String simpleSearch(String id) {
		
		//the enumerator of value list
		DocsEnum value_enum;
		//create a term for query
		Term qterm = new Term(fieldname1,id);
		try {
			value_enum = areader.termDocsEnum(qterm);
			return indexReader.document(value_enum.nextDoc()).getField(fieldname2).stringValue();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;	
	}
	
	public void closeWriter() throws Throwable
	{
		MMwriter.close();
	}
	
}


