package test;
import kv.*;

public class strKey extends Key{

	public String key;
	
	strKey(String key) {
		this.key = key;
	}
	
	@Override
	public int compare(Key K) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getType() {
		// TODO Auto-generated method stub
		return this.STR_KEY;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return key;
	}

}
