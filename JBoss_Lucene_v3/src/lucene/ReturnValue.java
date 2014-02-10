package lucene;
import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;

/**
 * This is the class returned for each query  
 * */
public class ReturnValue implements Serializable{

	//String is element id
	//double[0] is the count; double[1] is the accumulate distance
	public HashMap<String, Double[]> table;
	//the current element id
	public Vector<String>  current_values;
	//the query string
	public String querystring;
	//store the position of previous scan process
	public int pre_position;
	
	public ReturnValue () {
		
		this.querystring = "";
		this.pre_position = -1;
		this.table = new HashMap<String, Double[]>();
		this.current_values = new Vector<String>();
	}
	
	public List<Map.Entry<String, Double[]>> sortedOncount() {
		
		List<Map.Entry<String, Double[]>> infoIds = 
		    new ArrayList<Map.Entry<String, Double[]>>(this.table.entrySet());
		
		String result = "";		
		//sort the hashmap 
		//by count; then by id
		Collections.sort(infoIds, new Comparator<Map.Entry<String, Double[]>>() {   
		    public int compare(Map.Entry<String, Double[]> o1, Map.Entry<String, Double[]> o2) {   
		    	int delta =(int) (o2.getValue()[0] - o1.getValue()[0]);
		        if(delta != 0)
		        	return delta;
		        else
		        	return Integer.valueOf(o1.getKey()) - Integer.valueOf(o2.getKey()); 
		    }
		}); 

		result = "";
		//print for test
		/*for (int i = 0; i < infoIds.size(); i++) {
			Entry<String, Double[]> entry = infoIds.get(i);
		    result += "[" + entry.getKey() + ", " + entry.getValue()[0] + ", "  + entry.getValue()[1] + "]";		    
		}
		System.out.println(result);*/
		return infoIds;
	}
	
	public List<Map.Entry<String, Double[]>> sortedOndis() {
		
		List<Map.Entry<String, Double[]>> infoIds = 
		    new ArrayList<Map.Entry<String, Double[]>>(this.table.entrySet());
		
		String result = "";
		//sort the hashmap
		//by distance; then by id
		Collections.sort(infoIds, new Comparator<Map.Entry<String, Double[]>>() {   
		    public int compare(Map.Entry<String, Double[]> o1, Map.Entry<String, Double[]> o2) { 
		    	if(o1.getValue()[1] > o2.getValue()[1])
		    		return 1;
		    	else if(o1.getValue()[1] < o2.getValue()[1])
		    		return -1;
		        else
		        	return Integer.valueOf(o1.getKey()) - Integer.valueOf(o2.getKey()); 
		    }
		}); 
	
		//for test
		/*for (int i = 0; i < infoIds.size(); i++) {
			Entry<String, Double[]> entry = infoIds.get(i);
		    result += "[" + entry.getKey() + ", " + entry.getValue()[0] + ", "  + entry.getValue()[1] + "]";		    
		}
		System.out.println(result);*/
		return infoIds;
	}
	
	public void merge(ReturnValue value) {
		
		//link the query string
		this.querystring += (" "+value.querystring);
		//merge the hash map
		List<Map.Entry<String, Double[]>> infoIds = 
		    new ArrayList<Map.Entry<String, Double[]>>(value.table.entrySet());
		
		for(int i = 0; i < infoIds.size(); i++) {
			String key = infoIds.get(i).getKey();
//			System.out.println("In merge: "+key);
			if(this.table.containsKey(key)) {
				
				Double count_dis1[] = this.table.get(key);
				Double count_dis2[] = infoIds.get(i).getValue();
				//add the count
				count_dis1[0] += count_dis2[0];
				//add the distance
				count_dis1[1] += count_dis2[1];
				this.table.put(key, count_dis1);
			}
			else {
				
				Double count_dis[] = infoIds.get(i).getValue();
				this.table.put(key, count_dis);
			}
		}
	}
}
