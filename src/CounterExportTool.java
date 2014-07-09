import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class CounterExportTool {
	public static String phoenixDriver = "org.apache.phoenix.jdbc.PhoenixDriver";
	public static String dev_zookeeper = "115.29.137.169";
	
	public static void main(String args[]) {
		
		String filePath = "conf/properties.txt";
		Properties props = new Properties();
		try {
			// make sure we can load the driver
			Class.forName(phoenixDriver);
			InputStream in = new BufferedInputStream(new FileInputStream(
					filePath));
			props.load(in);
			String value = props.getProperty("table");
			System.out.println(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
