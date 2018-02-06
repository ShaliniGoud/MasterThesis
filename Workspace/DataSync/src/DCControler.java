import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.net.*;
import java.util.Scanner;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextField;

class JobEntry {
	String sKey;
	String sValue;
	String ServerIp;
	int portno;
	String dtype;
	int priority;
	int bandwidth;
}

public class DCControler  extends JFrame implements ActionListener {

	class JobQue
	{
		JobEntry [] jl = new JobEntry[10];
		int btm,qsiz;
		JobQue()
		{
			btm = 0;
			qsiz =10;
		}
		
		int  addjob(JobEntry je){
			
			System.out.println("itemid:"+btm);
			if ( btm < qsiz){
			jl[btm] = new JobEntry();
					
			jl[btm].ServerIp = je.ServerIp ;
			jl[btm].portno = je.portno ;
			jl[btm].dtype = je.dtype; 
			jl[btm].sKey = je.sKey;
			jl[btm].sValue = je.sValue;
					
			btm++;
			System.out.println("job Submitted "+je.portno+" "+ je.ServerIp+" "+ je.sKey );
			
			return 1;
			}
			else return 0;
		}
		
		JobEntry NextShedulejob(){
			JobEntry je = null;
			//shedule algorithem must go here
			if (btm>0) {
				btm--;
				je = new JobEntry();
				
				je.ServerIp = jl[btm].ServerIp;
				je.portno =  jl[btm].portno ;
				je.dtype =  jl[btm].dtype ; 
				je.sKey=  jl[btm].sKey;
				je.sValue =  jl[btm].sValue;
				System.out.println("job Completed"+je.portno+" "+ je.ServerIp+" "+ je.sKey);
			}
			return je;
		}
	}

	ObjectOutputStream op;
	Socket rqstSoc;
	static int Max_servers =1;
	public static JobQue jq;
	JLabel l1,l2,l3,l4,l5,l6,l7,l8,l9,l10,head;
	JTextField t1,t2,t3,t4,t7,t16,t17,t18,t19;
	JButton b1,b2,b3,b4,b5,b6,b7,b10;		

	class SyncJobSheduler implements Runnable {
		ServerSocket ser;
		Socket con;
		public String stLine;
		int prt;
		String SyncReqIP,jobstatus;
		public SyncJobSheduler(int prt) {
			this.prt = prt;
			stLine = "waiting for client to get connected";
		}
		public void run() {
			
			System.out.println("JobSheduler - stared");
				while (true) {				
					JobEntry je = jq.NextShedulejob();
					//System.out.println("Sync Co");
					if (je !=null) {
					
					//send shedule info to datacenter
					Socket requestSocket =null;
					ObjectOutputStream out = null;
					try{
						//1. Creating a socket to connect to the peer server
						requestSocket = new Socket(je.ServerIp,je.portno +1);
						System.out.println("Sync Command Sent to Server"+ je.ServerIp+"("+je.portno+") "+ je.sKey );
						//2. To Get Input and Output streams
						out = new ObjectOutputStream(requestSocket.getOutputStream());
						out.flush();
						String sendmsg = je.sKey +" "+ je.sValue +" "+je.dtype;
						out.writeObject(sendmsg );
						out.flush();
						out.close();
								
					} catch(UnknownHostException unknownHost){                                             //To Handle Unknown Host Exception
						System.err.println("You are trying to connect to an unknown host!");
					} catch (Exception e) {
						e.printStackTrace();
					}
					finally{
						//4: Closing connection
						try{
							out.close();
							requestSocket.close();
						}
						catch(IOException ioException){
							ioException.printStackTrace();
						}
					}
					}
					//commend to wait
					for (int i =0; i<100;i++);
				}

					//end----
				}
	}
		
					
	class SyncReqLis implements Runnable {
		
		ServerSocket ser;
		Socket con;
		public String stLine;
		int prt;
		String SyncReqIP,jobstatus;

		public SyncReqLis(int prt) {
			this.prt = prt;
			stLine = "waiting for client to get connected";
		}
	//run method starts
		public void run() {
			try {
				ser = new ServerSocket(prt);

				while (true) {
					con = ser.accept();	//waitng for request from client
					SyncReqIP = con.getInetAddress().getHostAddress();
					
					ObjectInputStream ob = new ObjectInputStream(con.getInputStream());
					stLine = (String)ob.readObject();

					String[] temp;
					temp = stLine.split(" ");
					
					JobEntry je = new JobEntry();
					
					je.ServerIp = SyncReqIP;
					je.portno =   Integer.parseInt(temp[0]);
					je.dtype = temp[1]; 
					je.sKey= temp[2];
					je.sValue = temp[3];
//					System.out.println("Job Submited From:"+SyncReqIP );
					jq.addjob(je);
	
					//op.flush();
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
	DCControler () {
		super("SDN Controler");
		setLayout(null); 
		
		jq = new JobQue();
		Thread t2 = new Thread (new SyncReqLis(5001));
		t2.setName("SyncReqLis");
		t2.start();
		
		head=new JLabel("SDN Controler");
		head.setBounds(270,30,600,50);
		head.setFont(new Font("Courier",Font.BOLD,30));
		add(head);

		b1=new JButton("Exit");
		b1.setBounds(550,100,100,30);
		add(b1);		
		b1.addActionListener(this);

		
		int pid1 =0;	
		try {
		FileReader fr = new FileReader("config.ini");//read the filename in to filereader object    
	    String val=new String();
		BufferedReader br = new BufferedReader(fr);	
		
		val=br.readLine();
		{
			String[] temp;
			temp = val.split(" ");
			pid1 = Integer.parseInt(temp[1]);	
			
		}
		br.close();
		fr.close();
		} catch(Exception e){
				System.out.println("Could not read config.ini");
		}

		Thread t3 = new Thread (new SyncReqLis(pid1));
		t3.setName("SyncReqLis");
		t3.start();

		Thread t4 = new Thread (new SyncJobSheduler(pid1+1));
		t4.setName("SyncJobSheduler");
		t4.start();
		

}

public static void main(String[] args) {
	
	DCControler cntroler = new DCControler();
	cntroler.setBounds(0,0,800,600);
	cntroler.setVisible(true);
}

@Override
public void actionPerformed(ActionEvent e) {
	// TODO Auto-generated method stub
	if (e.getSource()==b1)	{		
    	System.out.println("Exiting.");
    	setVisible (false);
		dispose();
	    System.exit(0);
	}
}
}

