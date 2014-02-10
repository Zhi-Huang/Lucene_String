package readpeer;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import jboss.*;

public class Server {
	public static void main(String args[]) throws Throwable {
		
		JServer jserver = new JServer("socket://localhost:8888");
		jserver.init();
		jserver.start();
	}
}
