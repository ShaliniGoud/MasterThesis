import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.io.*;
import java.net.*;
import java.util.Random;
import java.util.Scanner;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextField;

class ServerTableEntry
{
    String ServerIp;
    int portno;
}

public class DCClient   extends JFrame implements ActionListener {
	
	ObjectOutputStream op;
	Socket rqstSoc;
	ServerTableEntry[] servertble = new  ServerTableEntry[20];
	static int Max_servers = 1;

	JLabel l1,l2,l3,l4,l5,l6,l7,l8,l9,l10,head;
	JTextField t1,t2,t3,t4,t7,t16,t17,t18,t19;
	
	JButton b1,b2,b3,b4,b5,b6,b7,b10;		

	//Main method
	
    public void actionPerformed(ActionEvent e)
    {
        String action = e.getActionCommand();       
        if (e.getSource()==b10)	{		
        	System.out.println("Exiting.");
        	setVisible (false);
			dispose();
		    System.exit(0);
		} else if (e.getSource()==b2){
	        System.out.println("SETKEY.");
	       PutOperation(); 
	    } else if (e.getSource()==b3){
	        System.out.println("GETKEY.");
	        GetOperation();
	    } else if (e.getSource()==b6){
	        System.out.println("DELETE.");
	        DeleteOperation(); 
	    }    
     }
	public static void main(String[] args) {
		DCClient clnt = new DCClient(); 
		clnt.setVisible(true);
		clnt.setBounds(0,0,800,600);
	    }

	int GetServerSqNoFromKey(String Key)
	{
		int c;
		Random t = new Random();
		c = t.nextInt(Max_servers-1);
		c++;
		return c;
	}

	public DCClient(){
		
		super("DATA CENTER CLIENT");
		setLayout(null); 

		
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
				ServerTableEntry entry = new ServerTableEntry();
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
	
		
		head=new JLabel("DATA CENTER CLIENT");
		head.setBounds(270,30,600,50);
		//head.setFont(new Font("Courier",Font.BOLD,30));
		add(head);
						
		l2=new JLabel("KEY");
		l2.setBounds(150,200,100,30);
		add(l2);
	
		t2=new JTextField();
		t2.setHorizontalAlignment (JTextField.RIGHT);
		t2.setBounds(280,200,170,25);	
		add(t2);
		t2.addKeyListener (new KeyAdapter () {
			public void keyTyped (KeyEvent ke) {
				char c = ke.getKeyChar ();
				if (! ((Character.isDigit (c)) || (c == KeyEvent.VK_BACK_SPACE))) {
					getToolkit().beep ();
					ke.consume ();
				}
			}
		}
		);
		
		l3=new JLabel("VALUE");
		l3.setBounds(150,230,100,30);
		add(l3);
	
		t3=new JTextField();
		t3.setHorizontalAlignment (JTextField.RIGHT);
		t3.setBounds(280,230,170,25);
		add(t3);


		
		b2=new JButton("<Set>");
		b2.setBounds(550,200,100,30);
		add(b2);		
		b2.addActionListener(this);

		b3=new JButton("<Get>");
		b3.setBounds(550,230,100,30);
		add(b3);		
		b3.addActionListener(this);
		
		b6=new JButton("<Delete>");
		b6.setBounds(550,260,100,30);
		add(b6);		
		b6.addActionListener(this);

		b10=new JButton("EXIT");
		b10.setBounds(550,450,100,30);
		add(b10);
		
		b10.addActionListener(this);

	}
	
	/*
//import java.util.Scanner;

//}
 * */	
public void PutOperation()
{
	//read key and value pair from console
	String HashKey = "";
	String KeyValue = "" ;
	
	String sendmsg = t2.getText() + " "+t3.getText();  	
	sendmsg = "PUT 0 "+ sendmsg;
	
	String[] temp;
	temp = sendmsg.split(" ");
	int sersqno = GetServerSqNoFromKey(temp[1]);
   	
	try{
   			rqstSoc = new Socket(servertble[sersqno].ServerIp,servertble[sersqno].portno );
			System.out.println("Connected to Server id:" +sersqno );
			//2. get Input and Output streams
			op= new ObjectOutputStream(rqstSoc.getOutputStream());
			op.flush();			
			op.writeObject(sendmsg);
			op.flush();
	
			ObjectInputStream ob = new ObjectInputStream(rqstSoc.getInputStream());
			String stLine = (String)ob.readObject();			
			System.out.println( "Put Operation:"+stLine);
			t3.setText(stLine);
		}
	catch(UnknownHostException unknownHost){
		System.err.println("trying to connect to an invalid host!");
	}
	catch(IOException ioException){
		ioException.printStackTrace();
	} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	finally{
		//4: Closing connection
		try{
			op.close();
			rqstSoc.close();
		}
		catch(IOException ioException){
			ioException.printStackTrace();
		}
	}
}
public void GetOperation()
{
	String HashKey = "";
	String KeyValue = "" ;

	HashKey = t2.getText();
	String sendmsg = "GET 0 "+HashKey;
	
	String[] temp;
	temp = sendmsg.split(" ");
	int sersqno = GetServerSqNoFromKey(temp[1]);
	
	try{
			
		//1. creating a socket to connect to the Index server
		rqstSoc =  new Socket(servertble[sersqno].ServerIp,servertble[sersqno].portno );
		System.out.println("Connected to Serve Id:"+sersqno );
		//2. get Input and Output streams
		op = new ObjectOutputStream(rqstSoc.getOutputStream());
		op.flush();			
		op.writeObject(sendmsg);
		op.flush();
		
		ObjectInputStream ob = new ObjectInputStream(rqstSoc.getInputStream());
		String stLine = (String)ob.readObject();
		
		System.out.println( "Get Operation:"+stLine);
		t3.setText(stLine);

		}
	catch(UnknownHostException unknownHost){
		System.err.println("trying to connect to an invalid host!");
		
	}
	catch(IOException ioException){
		ioException.printStackTrace();
	} catch (ClassNotFoundException e) {
		e.printStackTrace();
	}
	finally{
		//4: Closing connection
		try{
			op.close();
			rqstSoc.close();
		}
		catch(IOException ioException){
			ioException.printStackTrace();
		}
	}
}

public void DeleteOperation()
{
        
   	String HashKey = "";
	String KeyValue = "" ;

	System.out.println("Enter Key for Delete Operation");
//	Scanner in1 = new Scanner(System.in);//input to take filename to be searched
	HashKey =t2.getText();
	String sendmsg = "DEL 0 "+ HashKey;
	
	String[] temp;
	temp = sendmsg.split(" ");
	int sersqno = GetServerSqNoFromKey(temp[1]);

	try{
		//1. creating a socket to connect to the Index server
		rqstSoc =  new Socket(servertble[sersqno].ServerIp,servertble[sersqno].portno );
		System.out.println("Connected to Server ID:"+sersqno );
		//2. get Input and Output streams
		op = new ObjectOutputStream(rqstSoc.getOutputStream());
		op.flush();			
		op.writeObject(sendmsg);
		op.flush();
		
		ObjectInputStream ob = new ObjectInputStream(rqstSoc.getInputStream());
		String stLine = (String)ob.readObject();
		
		System.out.println( "DEL Operation:"+stLine);
		t3.setText(stLine);
		}
	catch(UnknownHostException unknownHost){
		System.err.println("trying to connect to an invalid host!");
	}
	catch(IOException ioException){
		ioException.printStackTrace();
	} catch (ClassNotFoundException e) {
		e.printStackTrace();
	}

	finally{
	//4: Closing connection
		try{
			op.close();
			rqstSoc.close();
		}
		catch(IOException ioException){
			ioException.printStackTrace();
		}
	}
}
}

