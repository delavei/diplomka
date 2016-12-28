import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;


public class Generator {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException, IOException {
		/*PrintWriter tsdbPut;
		Socket echoSocket = new Socket("localhost", 4242);
		tsdbPut = new PrintWriter(echoSocket.getOutputStream(), true);*/
		
		PrintWriter writer = new PrintWriter("/home/juraj/opentsdb_import", "UTF-8");
		
		long k;
		String putQuery;
		Random rand = new Random();
		int t= 1;
		double load=0;
		String direction = "up";
		double bla=0;
		long counter=0;
		for (int i=1;i<=t;i++) {
			for (int j=1;j<=t;j++) {
				//for (k= 1482883786L; k<1482883986L; k=k+5) { /* 200 sekund*/
				//for (k= 1482883786L; k<1482983786L; k=k+5) { /* den*/
				for (k= 1479945600L; k<1482983786L; k=k+20) { /* mesiac*/
				
				//for (i= 1480278181574; i<1482865638250; i=i+5) {
				//for (i= 1482865538250; i<1482865638250; i=i+5) {
					
					if (counter % 10 == 0) {
						bla++;
					}
					counter++;
					
					load = (Math.sin(Math.toRadians(bla)))*100;
					 
					putQuery = "docker.cpu.load " + k + " "  
							+ load + " host=" + i + " image=" + j;
					writer.println(putQuery);
					//System.out.println(putQuery);
				}
				System.out.println("i " + i + "    j " + j);
			}			
		}
		
	}

}
