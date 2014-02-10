package kv;

import java.io.Serializable;

public class KVPair implements Serializable{
	public Key key;
	public Value value;
	
	public KVPair(Key k, Value v) {
		key = k;
		value =v;
	}
}
