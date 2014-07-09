import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

public class CounterExportTool {
	private static String phoenix_driver = "org.apache.phoenix.jdbc.PhoenixDriver";
	private static String property_file = "settings.properties";
	private static String env;
	private static String table;
	private static String appid;
	private static String counterid;
	private static Date startTime;
	private static Date endTime;
	private static String outputFile;
	private static boolean getCountOnly;
	private static Connection conn;

	public static void main(String args[]) {
		init();
//		createPhoenixConn();
//		execute();
//		cleanup();
	}
	
	private static void execute()
	{
		String startStr = getUnixTimeStringFromDate(startTime);
		String endStr = getUnixTimeStringFromDate(endTime);
		if (getCountOnly)
		{
			String sql = "select count(*) from \""+table +"\" where \"info\".\"appid\" = '"+appid+ "' and \"info\".\"counterid\" = '"+ counterid+ "' and \"info\".\"time\"> '"+ startStr+"' and \"info\".\"time\"< '"+ endStr+ "'";
			System.out.println();
			System.out.println("Executing sql query: "+ sql); 
			System.out.println("This may take a while to finish");
			System.out.println("...");
			try {
					Statement statement = conn.createStatement();
					ResultSet result = statement.executeQuery(sql);
					result.next();
					int size = result.getInt(1);
					System.out.println ("---------------------------");
					System.out.println (size + " records are found");
				} catch (SQLException e) {
					e.printStackTrace();
				}	
		}
		else
		{
			String sql = "select count(*) from \""+table +"\" where \"info\".\"appid\" = '"+appid+ "' and \"info\".\"counterid\" = '"+ counterid+ "' and \"info\".\"time\"> '"+ startStr+"' and \"info\".\"time\"< '"+ endStr+ "'";
			System.out.println();
			System.out.println("Executing sql query: "+ sql); 
			System.out.println("This may take a while to finish");
			System.out.println("...");

			try {
				BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
				Statement statement = conn.createStatement();
				ResultSet result = statement.executeQuery(sql);
				int count = 0;
				while (result.next())
				{
					count ++;
					int size = result.getMetaData().getColumnCount();
					String str = "";
					for (int i=0;i<size;i++)
						str+= result.getString(i)+" ";
					out.write(str+"\n");
				}
				System.out.println(count + " records are found");
				System.out.println("All records have been written into "+ outputFile);
			} catch (Exception e) {
				e.printStackTrace();
			}	
		
		}
	}
	
	private static void cleanup()
	{
		try {
			if (conn!=null && !conn.isClosed())
				conn.close();
		} catch (SQLException e) {

		}
	}
	
	private static void createPhoenixConn()
	{
		try {
			Class.forName(phoenix_driver);
		} catch (ClassNotFoundException e) {
			exit("FAILED: Cannot load phoenix driver");
		}
		String jdbcString = "jdbc:phoenix:"+env;
		try {
			conn = DriverManager.getConnection(jdbcString);
		} catch (SQLException e) {
			exit("FAILED: Cannot create connection to "+jdbcString);
		}
	}

	private static void init() {
		System.out.println("Reading property file ...");
		Properties props = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(
					property_file));
			props.load(in);
			env = loadProperty(props, "environment");
			table = loadProperty(props, "table");
			appid = loadProperty(props, "appid");
			counterid = loadProperty(props,"counterid");
			String start = loadProperty(props, "start");
			startTime = convertToBeijingDate(new Date(start));
			String end =  loadProperty(props, "end");
			endTime = convertToBeijingDate(new Date(end));
			outputFile = loadProperty(props, "outputfile");
			String getCount = props.getProperty("getCountOnly", "false");
			if (getCount.equalsIgnoreCase("true"))
				getCountOnly =true;
			File file = new File (outputFile);
			if (!file.exists())
			{
				try {
					file.createNewFile();
				} catch (IOException e) {
					exit ("FAILED: Cannot find or create output file at "+ outputFile);
				}
			}
		System.out.println("Finished loading property file");
		System.out.println();
		System.out.println("Connecting to zoopkeeper at "+env);
		System.out.println("Searching in hbase table "+table);
		System.out.println("Looking for appid:"+appid+", counterid:"+counterid);
		System.out.println("Time period is from "+startTime+" to "+endTime);
		} catch (FileNotFoundException e) {
			String message = "FAILED: Cannot find property file";
			exit(message);
		} catch (IOException e) {
			String message = "FAILED: Cannot load property file";
			exit(message);
		}
	}
	
	private static String loadProperty(Properties props, String propName){
		String prop = props.getProperty(propName);
		
		if (prop==null || prop.isEmpty())
		{
			exit ("FAILED: required property "+propName+ " is not defined in properties.txt");
		}
		return prop;
	}

	private static void exit(String message) {
		System.out.println(message);
		System.exit(1);
	}
	
	private static Date convertToBeijingDate(Date date)
	{
		DateFormat dfm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		String time = dfm.format(date);
		dfm.setTimeZone(TimeZone.getTimeZone("GMT+8:00"));
		try {
			Date beijing =  dfm.parse(time);
			return beijing;
		} catch (ParseException e) {
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}
	
	private static String getUnixTimeStringFromDate (Date date)
	{
		return "" + date.getTime()/1000;
	}
}
