import java.io.*;
import java.net.*;
import java.sql.*;
import java.security.MessageDigest;

public class FlickrFtpd extends Thread {

	// server config
  	private static String upload_root = "/path/to/flickr.com/temp";	// where to store uploaded files
	private static String ingest_path = "/usr/bin/php -q /path/to/flickr.com/sendto/ftp_process.gne";
	private static int localPort = 9021;		// port to listen on
	private static boolean debug = true;		// print stack traces
	private static boolean log   = true;		// show stuff

	// per instance class variables
	private Socket incoming;              

	// global class variables
	private static FlickrFtpd loadedServer;
	private ServerSocket server;
	private ServerSocket passiveSocket;
	private Socket dataSocket;
	private InetAddress remoteNode;
	private int remotePort = 1;

	// constants
	static final String XFER_COMPLETE = "226 transfer complete";
	private static final String BINARY_XFER   = "150 Binary data connection";
	private static final String COMMAND_OK    = "200 command succesful ";
	private static final String FAULT         = "550 ";
	private static final String NOT_LOGGED_IN = "530 Not logged in";
	private static final String FILE_OK       = "250 file action okay";
	private static final int inactivityTimer  = 5 * 60 * 1000;
	static final String TELNET = "ISO-8859-1";

	// db stuff
	protected Connection		db_conn		= null;
	protected ResultSet		db_rs		= null;
	protected Statement		db_stmt		= null;
	protected PreparedStatement	db_pstmt	= null;
	protected static String		db_url		= "jdbc:mysql://localhost/flickr?user=ftp-rw";

	// misc
  	static ThreadGroup tg = new ThreadGroup( "FlickrFtpd");
	static boolean shutdown = false;  

  
	public static void main(String[] args) {
		shutdown = false;
		new FlickrFtpd().start(); // kick off a (simulated) daemon thread
	}

	public static boolean kill() {
		int j=0;
		Thread meMySelfI = Thread.currentThread();
    
		do {
			/* iter across the thread group, killing all members. */
			shutdown = true;
			Thread list[] = new Thread[ tg.activeCount()];
      
			// get all members of the group (including submembers)
			int i = tg.enumerate( list);
      
			// no members means that we have gracefully suceeded
			if ( i == 0 ) return true;
      
			// if some of the threads do IO during the shut down they will
			// need time to accomplish the IO. So, I give it to 'em 
			// after the first attempt.
			if ( j > 0) try { meMySelfI.sleep( 500);} catch (Exception e) {};
      
			// try to shudown each thread in the group
			while ( i-- > 0) {
				FlickrFtpd tftp = (FlickrFtpd)list[i];
				tftp.interrupt();      // first, do it politely
				meMySelfI.yield();      // give 'em time to respond
				tftp.forceClose();     // second, use a big hammer
				meMySelfI.yield();      // give 'em time to respond
			}
		} while ( j++ <= 3);
		return false;
	}

	// set for a specific instance to notify runing as a (simulated) daemon thread.
	boolean isDaemon = false;

	public FlickrFtpd() {
		super( tg, null, "FtpDaemon");
		isDaemon = true;
	}

	public FlickrFtpd(Socket incoming) {
		super( tg, null, incoming.toString()); //~~ not a real good name....
		this.incoming = incoming;
	}

	private void daemon() {

		try {
			server = new ServerSocket(localPort);

			while (true) {
				Socket incoming = server.accept();
				new FlickrFtpd( incoming ).start();
			}
		}

		catch ( Exception e ) {   // usually network errors (including timeout)
			if ( server != null) 
				try { 
					server.close(); 
					server = null;
				}
				catch (Exception e1) {};
			if ( log) System.out.println( "forced Server exit "+e);
			if ( debug) e.printStackTrace();
		}
	}

	// initiate either a server or a user session
	public void run() {
		if (isDaemon) {
			daemon();
			return;
		};

		boolean loggedIn = false;
		int i, h1;
		String di,str1,user="unknown",user_id="0";
		InetAddress localNode;
		byte dataBuffer[] = new byte[1024];
		String command = null;
		StringBuffer statusMessage = new StringBuffer(40);
		File targetFile = null;

		try {
			// start mysql
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			this.db_conn = DriverManager.getConnection(db_url);
			this.db_stmt = this.db_conn.createStatement();

			this.db_stmt.executeUpdate("INSERT INTO test_table (name) VALUES ('hello world')");

			incoming.setSoTimeout(inactivityTimer);  // enforce I/O timeout
			remoteNode = incoming.getInetAddress();
			localNode = InetAddress.getLocalHost();

			BufferedReader in = new BufferedReader(new InputStreamReader(incoming.getInputStream(), TELNET));
			PrintWriter out = new PrintWriter(new OutputStreamWriter( incoming.getOutputStream(), TELNET),true);
			str1 = "220 Flickr FTP Server Ready";
			out.println(str1);
			if (log) System.out.println( remoteNode.getHostName()+" "+str1);

			boolean done = false;
			char dataType = 0;

			while ( !done ) {
				statusMessage.setLength(0);

				// obtain and tokenize command
				String str = in.readLine();
				if ( str == null ) break; // EOS reached
				i = str.indexOf( ' ');
				if ( i == -1 ) i = str.length();
				command = str.substring( 0, i).toUpperCase().intern();
				if ( log) System.out.print( user +"@"+remoteNode.getHostName()+" "+ (String)((command != "PASS")? str : "PASS ***"));
				str = str.substring( i).trim();

				try {
					if ( command == "USER" ) {

						user = str;
						statusMessage.append( "331 Password");

					} else if ( command == "PASS" ) {

						String pass = str;
						String pass_md5 = md5(pass);

						this.db_rs = this.db_stmt.executeQuery("SELECT * FROM users WHERE email='"+user+"' AND password='"+pass_md5+"'");
						if (this.db_rs.first()){
							loggedIn = true;
							user_id = this.db_rs.getString("id");
							System.out.println("Account id is "+user_id);
						}

						statusMessage.append(loggedIn?"230 logged in User":"530 Login Incorrect");

					} else if ( !loggedIn ) {

						statusMessage.append( "530 Not logged in");

					} else if ( command == "RETR" ) {

						statusMessage.append( "999 Not likely");

					} else if ( command == "STOR" ) {

						out.println( BINARY_XFER);

						// trim a leading slash off the filename if there is one
						if (str.substring(0, 1).equals("/")) str = str.substring(1);
						String filename = user_id + "_" + str;
						// TODO: sanitise filename
						targetFile = new File(upload_root + "/" + filename);

						RandomAccessFile dataFile = null;
						InputStream      inStream = null;
						OutputStream     outStream = null;
						BufferedReader br = null;
						PrintWriter pw = null;

						try {
							int amount;
							dataSocket = setupDataLink();

							// ensure timeout on reads.
							dataSocket.setSoTimeout( inactivityTimer);
              
							dataFile = new RandomAccessFile( targetFile, "rw");

							inStream = dataSocket.getInputStream();
							while ( (amount = inStream.read( dataBuffer)) != -1 ) dataFile.write( dataBuffer, 0, amount);

							statusMessage.append( XFER_COMPLETE);

							shell_exec(ingest_path + " " + user_id + " " + filename);
						}

						finally {
							try {if ( inStream   != null ) inStream.close();}
							catch ( Exception e1 ) {};
							try {if ( outStream  != null ) outStream.close();}
							catch ( Exception e1 ) {};
							try {if ( dataFile   != null ) dataFile.close();}
							catch ( Exception e1 ) {};
							try {if ( dataSocket != null ) dataSocket.close();}
							catch ( Exception e1 ) {};
							dataSocket = null;
						}

					} else if ( command == "REST" ) {

						statusMessage.append("502 Sorry, no resuming");

					} else if ( command == "TYPE" ) {

						if ( Character.toUpperCase( str.charAt( 0)) == 'I'){
							statusMessage.append( COMMAND_OK);
						} else {
							statusMessage.append( "504 Only binary baybee");
						}

					} else if (
						command == "DELE" || 
						command == "RMD" || 
						command == "XRMD" || 
						command == "MKD" || 
						command == "XMKD" || 
						command == "RNFR" || 
						command == "RNTO" || 
						command == "CDUP" || 
						command == "XCDUP" ||
						command == "CWD" ||
						command == "SIZE" ||
						command == "MDTM"
					) {

						statusMessage.append("502 None of that malarky!");

					} else if ( command == "QUIT" ) {

						statusMessage.append( COMMAND_OK).append( "GOOD BYE");
						done = true;

					} else if ( command == "PWD" | command == "XPWD" ) {

						statusMessage.append( "257 \"/\" is current directory");

					} else if ( command == "PORT" ) {

						int lng,lng1,lng2, ip2;
						String a1="",a2="";
						lng = str.length() - 1;
						lng2 = str.lastIndexOf(",");
						lng1 = str.lastIndexOf(",",lng2-1);

						for ( i=lng1+1;i<lng2;i++ ) {
							a1 = a1 + str.charAt(i);
						}

						for ( i=lng2+1;i<=lng;i++ ) {
							a2 = a2 + str.charAt(i);
						}

						remotePort = Integer.parseInt(a1);
						ip2 = Integer.parseInt(a2);
						remotePort = (remotePort <<8) + ip2;
						statusMessage.append( COMMAND_OK).append( remotePort);

					} else if ( command == "LIST" | command == "NLST" ) {

						try {

							out.println("150 ASCII data"); 
							dataSocket = setupDataLink();
              
							PrintWriter out2 = new PrintWriter( dataSocket.getOutputStream(),true);

							if ((command == "NLST")) {
								out2.println(".");
								out2.println("..");
							} else {
								out2.println("total 8.0k");
								out2.println("dr--r--r-- 1 owner group           213 Aug 26 16:31 .");
								out2.println("dr--r--r-- 1 owner group           213 Aug 26 16:31 ..");
							}

							// socket MUST be closed before signalling EOD
							dataSocket.close();
							dataSocket = null;
							statusMessage.setLength( 0);
							statusMessage.append( XFER_COMPLETE);
						}
						finally {
							try {if ( dataSocket != null ) dataSocket.close();}
							catch ( Exception e ) {};
							dataSocket = null;
						}

					} else if ( command == "NOOP" ) {

						statusMessage.append( COMMAND_OK);

					} else if ( command == "SYST" ) {

						statusMessage.append( "215 UNIX"); // allows NS to do long dir

					} else if ( command == "MODE" ) {

						if ( Character.toUpperCase( str.charAt( 0)) == 'S'){
							statusMessage.append( COMMAND_OK);
						} else {
							statusMessage.append( "504");
						}

					} else if ( command == "STRU" ) {

						if ( str.equals( "F") ) {
							statusMessage.append( COMMAND_OK);
						} else {
							statusMessage.append( "504");
						}

					} else if ( command == "PASV" ) {

						try {

							int num = 0, j = 0;
							if ( passiveSocket != null )  try { passiveSocket.close();} catch (Exception e) {};
							passiveSocket = new ServerSocket( 0); // any port

							// ensure timeout on reads.
							passiveSocket.setSoTimeout( inactivityTimer);
              
							statusMessage.append( "227 Entering Passive Mode (");
							String s = localNode.getHostAddress().replace( '.', ',');// get host #
							statusMessage.append( s).append( ',');
							num = passiveSocket.getLocalPort();// get port #
							j = (num >> 8) & 0xff;
							statusMessage.append( j);
							statusMessage.append( ',');
							j = num & 0xff;
							statusMessage.append( j);
							statusMessage.append( ')');
						}
						catch ( Exception e) {
							try {if ( passiveSocket != null ) passiveSocket.close();}
							catch ( Exception e1 ) {};
							passiveSocket = null;
							throw e;
						}

					} else {
						statusMessage.append( "502 unimplemented ").append( command);
					}
				}

				// shutdown causes an interruption to be thrown
				catch ( InterruptedException e)   { throw( e); }
    
				catch ( Exception e ) // catch all for any errors (including files)
				{
					statusMessage.append( FAULT).append( e.getMessage());
					if ( debug ) {
						System.out.println( "\nFAULT - lastfile "+targetFile);
						e.printStackTrace();
					};
				}

				// send result status to remote
				out.println( statusMessage);
				if ( log) System.out.println( "\t" + statusMessage);
			}
		}

		catch ( Exception e )   // usually network errors (including timeout)
		{
			if ( log) System.out.println( "forced instance exit "+e);
			if ( debug) e.printStackTrace();		
		}

		finally // exiting server instance
		{
			// tear down mysql
			if (this.db_rs    != null) { try { this.db_rs.close();    } catch (SQLException SQLE) { ; } }
			if (this.db_stmt  != null) { try { this.db_stmt.close();  } catch (SQLException SQLE) { ; } }
			if (this.db_pstmt != null) { try { this.db_pstmt.close(); } catch (SQLException SQLE) { ; } }
			if (this.db_conn  != null) { try { this.db_conn.close();  } catch (SQLException SQLE) { ; } }

			forceClose();
		}
	}


	private void forceClose() {

		// make sure network resources are released and that remote end
		// knows we are going away.

		if (incoming != null ) 
		try  {
			incoming.close(); 
			incoming = null;
		} 
		catch ( Exception e ) {};


		if (passiveSocket != null ) 
		try { 
			passiveSocket.close(); 
			passiveSocket = null;
		} 
		catch ( Exception e ) {};


		if (dataSocket != null ) 
		try { 
			dataSocket.close(); 
			dataSocket = null;
		} 
		catch ( Exception e ) {};
    

		if ( isDaemon && server != null)
		try {
			server.close();
			server = null;
		}
		catch ( Exception e) {};

	}
  
	private final Socket setupDataLink() throws java.io.IOException {

		Socket dataSocket =  ( passiveSocket != null) ? passiveSocket.accept() : new Socket( remoteNode, remotePort);

		// ensure timeout on reads.
		dataSocket.setSoTimeout( inactivityTimer);
    
		return dataSocket;
	}

	private String md5(String data){

		StringBuffer sb = new StringBuffer();

		try {
			MessageDigest messageDigest = MessageDigest.getInstance("MD5");
			messageDigest.update(data.getBytes());
			byte[] digestBytes = messageDigest.digest();

			/* convert to hexstring */
			String hex = null;

			for (int i = 0; i < digestBytes.length; i++) {
				hex = Integer.toHexString(0xFF & digestBytes[i]);

				if (hex.length() < 2) {
					sb.append("0");
				}
				sb.append(hex);
			}
		}
		catch (Exception ex) {
			System.out.println(ex.getMessage());
		}

		return sb.toString();
	}


	private String shell_exec(String cmdline) {
		String line = "";
		try {

			// windows
		 	//Process p = Runtime.getRuntime().exec(cmdline);
			// linux
			Process p = Runtime.getRuntime().exec(new String[] { "/bin/sh", "-c", cmdline });

		 	BufferedReader input = new BufferedReader (new InputStreamReader(p.getInputStream()));
		 	while ((line += input.readLine()) != null) {
		   	}
		 	input.close();
		} catch (Exception err) {
		 	err.printStackTrace();
		}
		return line;
	}	

}
