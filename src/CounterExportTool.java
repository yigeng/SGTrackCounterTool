import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;



public class CounterExportTool {
	private static String property_file = "settings.properties";
	private static String table;
	private static String query_url;
	private static String appid;
	private static String counterid;
	private static String publisherid;
	private static String channelid;
	private static String platformid;
	private static Date startTime;
	private static Date endTime;
	private static String outputDir;
	private static String exclude_publisher;
	private static String server_url = "114.215.105.66";
	private static int line_per_file = -1;
	private static String all_counters;
	private static String all_publishers;

	public static void main(String args[]) throws ClientProtocolException, IOException {
		init();
		query();
	}
	
	private static void query() throws ClientProtocolException, IOException
	{
		String startStr = getUnixTimeStringFromDate(startTime);
		String endStr = getUnixTimeStringFromDate(endTime);
		
		if (query_url!=null && query_url.length()>0)
			server_url = query_url;
		
		if (exclude_publisher.equalsIgnoreCase("true"))
			exclude_publisher = "1";
		else
			exclude_publisher = "0";
		
		String url = "http://"+server_url+":8080/sgpromo_ssh/searchbypublisher?tablename="+table+"&appid="+appid+"&counterid="+counterid+"&publisherid="+publisherid+"&starttime="+startStr+"&endtime="+endStr+"&exclude="+exclude_publisher;
		
		if (all_counters!=null && all_counters.equalsIgnoreCase("true"))
			url = "http://"+server_url+":8080/sgpromo_ssh/searchallcountersbypublisher?tablename="+table+"&appid="+appid+"&publisherid="+publisherid+"&starttime="+startStr+"&endtime="+endStr+"&exclude="+exclude_publisher;
		
		if (all_publishers!=null && all_publishers.equals("true"))
			url = "http://"+server_url+":8080/sgpromo_ssh/searchitems?tablename="+table+"&appid="+appid+"&counterid="+counterid+"&starttime="+startStr+"&endtime="+endStr;
		
		  System.out.println("Calling "+ url);
		  System.out.println("Fetching data, please wait...");
		  CloseableHttpClient httpclient = HttpClients.createDefault();
          HttpGet httpget = new HttpGet(url);
          HttpResponse response = httpclient.execute(httpget); 
          HttpEntity entity = response.getEntity();
          String html = EntityUtils.toString(entity);  
  		  JSONParser parser = new JSONParser();
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
          sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
  		  try {
  			
			JSONObject record = (JSONObject)parser.parse(html);
			JSONArray payload = (JSONArray)record.get("data");
			int file_id = 1;
			int line_no = 0;
			BufferedWriter out= new BufferedWriter(new FileWriter(outputDir+"/result"+""+file_id+".csv"));
			Iterator<?> keys = payload.iterator();
			JSONObject item = null;
	        while( keys.hasNext() ){
	        	line_no ++;
	        	try{
		            item = (JSONObject)keys.next();
		            String metadata_str = (String)item.get("metadata");
		            JSONObject metadata = (JSONObject)parser.parse(metadata_str);
		            String userid = (String)item.get("userid");
		            if (userid==null || userid.equals("0"))
		            	continue;
		            String counterid = (String) item.get("counterid");
		            String timeStamp= (String)item.get("time");
		            Long timestamp = Long.parseLong(timeStamp);
		            TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
		            java.util.Date time=new java.util.Date(timestamp*1000);
		            String stamp = sdf.format(time);
		            String line = counterid+","+userid+","+ stamp +",";
		            Iterator<?> parts = metadata.keySet().iterator();
		            // 确保metadata keys按顺序输出
		            ArrayList<String> metadata_keys = new ArrayList<String>();	            
		            while (parts.hasNext())
		            {
		            	String part = (String)parts.next();
		            	metadata_keys.add(part);
		            }
		            Collections.sort(metadata_keys);
		            for (String key : metadata_keys)
		            	line += key+","+(String)metadata.get(key)+",";
		            
		            if (line_per_file >0 && line_no>line_per_file)
		            {
		            	out.flush();
		            	line_no = 0;
		            	file_id++;
		            	out =  new BufferedWriter(new FileWriter(outputDir+"/result"+""+file_id+".csv"));
		            }
		            //System.out.println(line);
					out.write(line+"\n");
	        	}catch (Exception e)
	        	{
	        		System.out.println("Encountered mal-formed data "+ e.toString());
	        		System.out.println(item+"\n________________");
	        	}
	        }
	        out.close();
	        System.out.println("=================== Done! ===================");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
          httpclient.close();
	}

	private static void init() {
		System.out.println("Reading property file ...");
		Properties props = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(
					property_file));
			props.load(in);
			table = loadProperty(props, "table");
			appid = loadProperty(props, "appid");
			query_url = loadOptionalProperty(props, "query_url");
			counterid = loadProperty(props,"counterid");
			publisherid = loadOptionalProperty(props, "publisherid");
			channelid = loadOptionalProperty(props, "channelid");
			platformid = loadOptionalProperty(props, "channelid");		
			String start = loadProperty(props, "start");
            TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
			startTime = new Date(start);
			String end =  loadProperty(props, "end");
			endTime = new Date(end);
			outputDir = loadProperty(props, "outputDir");
			exclude_publisher = loadOptionalProperty(props,"exclude_publisher");
			all_counters = loadOptionalProperty(props,"all_counters");
			all_publishers = loadOptionalProperty(props,"all_publishers");
			line_per_file = Integer.parseInt(loadOptionalProperty(props,"line_per_file"));

			File file = new File (outputDir);
			if (!file.exists())
			{
				file.mkdir();
			}
		System.out.println("Finished loading property file");
		System.out.println();
		System.out.println("Looking for appid:"+appid+", counterid:"+counterid);
		System.out.println("Exclude publisher id is "+ exclude_publisher +", publisher id is "+publisherid);
		System.out.println("Time period is from "+startTime+" to "+endTime);
		System.out.println();
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

	private static String loadOptionalProperty(Properties props, String propName){
		String prop = props.getProperty(propName);
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
