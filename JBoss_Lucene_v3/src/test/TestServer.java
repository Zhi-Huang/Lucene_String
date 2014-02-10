package test;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import jboss.*;

public class TestServer {
	public static void main(String args[]) throws Throwable {
//		BufferedReader buf = new BufferedReader(new InputStreamReader(System.in));
//		System.out.println("Input server locator:");
//		JServer jserver = new JServer(buf.readLine());
		JServer jserver = new JServer("socket://localhost:8888");
		jserver.init();
		jserver.start();
	}
}
