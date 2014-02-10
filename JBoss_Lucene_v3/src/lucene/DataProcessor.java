package lucene;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author huang zhi
 * this class provides functions for the format issue
 * */

public class DataProcessor {
	
	static public String process(int ngrams, String longgrams) {
		
		String shortgrams = "";
		longgrams = underline (longgrams);
		for (int i = 0; i < longgrams.length() - ngrams + 1; i++) {
			shortgrams += (longgrams.substring(i, i + ngrams) + " ");
		}
		return shortgrams;
	}
	
	/**
	 * get unique ngrams from a input string
	 * */
	static public String getGrams(int ngrams, String instr) {
		
		HashMap<String, String> map = new HashMap<String, String> ();
		String grams = "";
		for(int i = 0; i < instr.length() - ngrams + 1; i++) {
			String gram = instr.substring(i, i + ngrams);
			if(map.containsKey(gram) == false) {
				grams += (gram + "|");
				map.put(gram, null);
			}	
		}
		return grams;
	}
	
	/**
	 * dealing with the underline between words 
	 * */
	private static String underline(String line) {
		StringBuffer newstring = new StringBuffer();
		for(int i = 0; i < line.length(); i++)
			if(line.charAt(i) != '_')
				newstring.append(line.charAt(i));
		return newstring.toString();
	}
	
	/**
	 * format the integer
	 * e.g.   1 => 001 12 => 012
	 * */
	public static String intFormat(int a) {
		
		StringBuffer buf = new StringBuffer();
		for(int i = 0; i < 3 - String.valueOf(a).length(); i++) {
			buf.append("0");
		}
		return (buf.toString()+String.valueOf(a)); 
	}
	
	public static String strFormat(String str) {
		if(str.contains("+") == false) 
			return str;
		else {
			String values[] = new String[2];
			values = str.split("\\+");
			return intFormat(Integer.valueOf(values[0]))+"+"+intFormat(Integer.valueOf(values[1])); 
		}
	}
	
	
	/**
	 * get integer value from the combination: dim+value
	 * */
	public static int getSiftValue(String sift_feature) {
		
		Integer integer = -1;
		integer = Integer.valueOf(sift_feature.substring(sift_feature.lastIndexOf("+")+1, sift_feature.length()));
		return integer.intValue();
	}
	
	/**
	 * get integer dimension from the combination: dim+value
	 * */
	public static int getSiftDim(String sift_feature) {
		
		Integer integer = -1;
		integer = Integer.valueOf(sift_feature.substring(0, sift_feature.lastIndexOf("+")));
		return integer.intValue();
	}

	
	/**
	 * test the functions
	 * */
	public static void main (String a[]) throws IOException {
	
//		System.out.println(getSiftValue("1+001")+getSiftValue("11+012")+getSiftValue("111+112"));
//		System.out.println(getSiftDim("111+001")+getSiftDim("11+012")+getSiftDim("111+112"));
//		System.out.println(strFormat("82+49"));
		System.out.println(getGrams(3,"drag a video clip"));
	}
}
