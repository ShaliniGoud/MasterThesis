//DCServer.java

import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.io.*;
import java.net.*;

class ReqEntry {
	String sKey;
	String sValue;
    String dtype;
}

class ServerTableEntry1
{
    String ServerIp;
    int portno;
}

//PrClntSer starts
public class DCServer {

	ObjectOutputStream op;
	Socket rqstSoc;
	ServerTableEntry1[] servertble = new  ServerTableEntry1[20];
	static int Max_servers = 1;
	ReqEntry KeyValueTable[] = new ReqEntry [10000]; 
	int ser_seqno;
	
	//SyncWithMe 
	class SyncWithMe implements Runnable {
		
		ServerSocket ser;
		Socket con;
		public String stLine;
		int prt;

		public SyncWithMe (int prt) {
			this.prt = prt;
			stLine = "waiting for data";
		}

	//run method starts
		public void run() {
			try {
				ser = new ServerSocket(prt);

				while (true) {
					con = ser.accept();	//waitng for request from client		
					System.out.println("Update Request From Other Server:");    				   				
					ObjectInputStream ob = new ObjectInputStream(con.getInputStream());
					stLine = (String)ob.readObject();	
					System.out.println(stLine);
					String[] temp;
					temp = stLine.split(" ");
					int indx =  Integer.parseInt(temp[2]);

					if (temp[1].equalsIgnoreCase("2")) 
						KeyValueTable[indx] = null;
					else {
					KeyValueTable[indx] = new ReqEntry();
					KeyValueTable[indx].dtype =temp[1];
					KeyValueTable[indx].sKey =temp[2];
					KeyValueTable[indx].sValue = temp[3];				
					}
					ob.close();
					con.close();   				
				}
			} 
			
			catch(ClassNotFoundException classnot){
				System.err.println("Incorrect format");
			}
			catch(IOException ioException){
				ioException.printStackTrace();
			} 
		}
	}

	//SyncWithOtherServer 
	class SyncWithOtherServer implements Runnable {
		
		ServerSocket ser;
		Socket con;
		public String stLine;
		int prt;

		public SyncWithOtherServer (int prt) {
			this.prt = prt;
			stLine = "waiting Sync Command";
		}

		void SendSyncMessagetoOthers(ReqEntry kve)
		{
			Socket rqstSoc;
			try{
				//1. creating a socket to connect to the Index server
				for (int i = 1; i< Max_servers; i++) {
					if (i != ser_seqno ) {
						rqstSoc =  new Socket(servertble[i].ServerIp,servertble[i].portno+2 );
						System.out.println("Sent Update Request to Server:" + (servertble[i].portno+2) );
						//2. get Input and Output streams
						op = new ObjectOutputStream(rqstSoc.getOutputStream());
						op.flush();	
						String sendmsg;
						sendmsg = "SYNC "+kve.dtype +" "+ kve.sKey + " "+ kve.sValue;
						System.out.println(sendmsg);
						op.writeObject(sendmsg);
						op.flush();
						op.close();
						rqstSoc.close();
					}
				}
				}
			catch(UnknownHostException unknownHost){
				System.err.println("trying to connect to an invalid host!");
			}
			catch(IOException ioException){
				ioException.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	//run method starts
		public void run() {
			try {
				ser = new ServerSocket(prt);

				while (true) {
					con = ser.accept();	//waitng for request from client		
					System.out.println("CMD-SYNC from Controler");    				   				
					ObjectInputStream ob = new ObjectInputStream(con.getInputStream());
					stLine = (String)ob.readObject();	
					
					String[] temp;
					temp = stLine.split(" ");
					String retval = "";
	//sync with all servers
					ReqEntry kve = new ReqEntry();
					//int indx =  Integer.parseInt(temp[0]);
					kve.dtype = temp[2];
					kve.sKey = temp[0] ;
					kve.sValue =temp[1];						
					SendSyncMessagetoOthers(kve);

					ob.close();
					con.close();   				
				}
			} 
			
			catch(ClassNotFoundException classnot){
				System.err.println("Incorrect format");
			}
			catch(IOException ioException){
				ioException.printStackTrace();
			} 
		}
	}

	
	class PrtLtnSend implements Runnable {
		
		ServerSocket ser;
		Socket con;
		public String stLine;
		int prt;
		
		
		public PrtLtnSend(int prt) {
			this.prt = prt;
			stLine = "waiting for client to get connected";
		}
				
		void SendSyncReqToControler(ReqEntry re){
			Socket rqstSoc;
			try{
				//1. creating a socket to connect to the Index server
				rqstSoc =  new Socket(servertble[0].ServerIp,servertble[0].portno );
				System.out.println("Submit Job to Controler(Key):" + re.sKey);
				//2. get Input and Output streams
				op = new ObjectOutputStream(rqstSoc.getOutputStream());
				op.flush();	
				
				String sendmsg;
				sendmsg = servertble[ser_seqno].portno+" " +re.dtype +" "+ re.sKey + " "+ re.sValue;
				op.writeObject(sendmsg);
				op.flush();
				op.close();
				rqstSoc.close();
				
				}
			catch(UnknownHostException unknownHost){
				System.err.println("trying to connect to an invalid host!");
			}
			catch(IOException ioException){
				ioException.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	//run method starts
		public void run() {
			try {
				ser = new ServerSocket(prt);

				while (true) {
					con = ser.accept();	//waitng for request from client		
//					System.out.println("Connection  is received from " + con.getInetAddress().getHostName());    				   				
					ObjectInputStream ob = new ObjectInputStream(con.getInputStream());
					stLine = (String)ob.readObject();						
					String[] temp;
					temp = stLine.split(" ");
					String retval = "";
					if (temp[0].equalsIgnoreCase("PUT"))
					{			
						int indx =  Integer.parseInt(temp[2]);
						//indx = indx / Max_servers;
						ReqEntry kve = new ReqEntry();
						kve.dtype =  temp[1];
						kve.sKey = temp[2];
						kve.sValue = temp[3];	
						KeyValueTable[indx] = kve;
						retval = "PUT OK";
						System.out.println("PUT cmd received from client(Key)"+kve.sKey);
						SendSyncReqToControler(kve);
						
					} else if (temp[0].equalsIgnoreCase("GET")) {
						int indx =  Integer.parseInt(temp[2]);
						System.out.println("GET cmd received from client(Key)"+indx);
						
						if (KeyValueTable[indx] != null) {
						    retval =  KeyValueTable[indx].sValue; //"GET OK";
						} else {
							retval =  "value not found";
						}
						
					} else {
						int indx =  Integer.parseInt(temp[2]);
						System.out.println("DELETE cmd received from client(Key)"+indx);
						if (KeyValueTable[indx] != null) {
							KeyValueTable[indx] = null;
						    retval = "KEY DELETED OK";
							ReqEntry kve = new ReqEntry();
							kve.dtype = "2";
							kve.sKey = temp[2];
							kve.sValue = "No";	
						    SendSyncReqToControler(kve);
						} else {
							retval =  "KEY not found";
						}
						
					}
									 
					ObjectOutputStream op = new ObjectOutputStream(con.getOutputStream());
					op.flush();
					op.writeObject(retval);//writing back the response of downloaded file to the client
					op.flush();
					ob.close();
					con.close();   				
	 			}
			} 
			
			catch(ClassNotFoundException classnot){
				System.err.println("Incorrect format");
			}
			catch(IOException ioException){
				ioException.printStackTrace();
			} 
		}
	}
    //Constructor
    public DCServer() {
    	   	
		try
		{
			//config file must contains list of (ip-addres and portno) pair for all the peer server client
			FileReader fr = new FileReader("config.ini");//read the filename in to filereader object    
            String val=new String();
			BufferedReader br = new BufferedReader(fr);	
			int i = 0;			
			while((val=br.readLine())!=null)
			{
				String[] temp;
				temp = val.split(" ");
				ServerTableEntry1 entry = new ServerTableEntry1();
				entry.ServerIp = temp[0];
				entry.portno = Integer.parseInt(temp[1]);
				servertble[i] = entry;
				i++;
			}
			System.out.println("server table initialized:");
			Max_servers = i;
			br.close();
			fr.close();
		} catch(Exception e){
					System.out.println("Could not read config.ini");
		}
		
		System.out.println("Enter Serial(Sequence no) for PeerServerClient ID");
    	Scanner in1 = new Scanner(System.in);
    	String regmsg = in1.nextLine();
    	ser_seqno = Integer.parseInt(regmsg);
    	
    	//start server thread 
    	StartLissioners(servertble[ser_seqno].portno);		
    }
   

//Main method
public static void main(String[] args) {
	DCServer mf = new DCServer();    
	while (true){
    	System.out.println("press 1 -> Exit \n");
    	Scanner in1 = new Scanner(System.in);
    	String regmsg = in1.nextLine();
    	 
    	if (regmsg.equals("1")){
        	System.out.println("exiting...");
        	System.exit(0); 
        }
    }
}
//Thread for downloading
public void StartLissioners(int pid1)
    {   //request from client
		Thread t1 = new Thread (new PrtLtnSend(pid1));
		t1.setName("KeyValueServerService");
		t1.start();
		
		//request come from controler
		Thread t2 = new Thread (new SyncWithOtherServer(pid1+1));
		t2.setName("SyncWithOtherServer");
		t2.start();

		//request from other DC
		Thread t3 = new Thread (new SyncWithMe(pid1+2));
		t3.setName("SyncWithMe");
		t3.start();
		
    }
}
