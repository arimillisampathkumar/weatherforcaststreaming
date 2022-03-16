package edu.stream.spark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class WeatherDataExtractor implements Runnable {
	String propertiesFilePath = null;

	public WeatherDataExtractor(String propertiesFilePath) {
		this.propertiesFilePath = propertiesFilePath;
	}

	private static String processJSON2CSV(JSONObject json) {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

		String dateString = formatter.format(Calendar.getInstance().getTime());

		StringBuilder sb = new StringBuilder(dateString);
		sb.append(";");
		sb.append(json.getJSONObject("coord").get("lon")).append(";");
		sb.append(json.getJSONObject("coord").get("lat")).append(";");
		JSONArray arr = json.getJSONArray("weather");
		for (int i = 0; i < arr.length(); ) {
			sb.append(arr.getJSONObject(i).get("id")).append(";");
			sb.append(arr.getJSONObject(i).get("main")).append(";");
			sb.append(arr.getJSONObject(i).get("description")).append(";");
			sb.append(arr.getJSONObject(i).get("icon")).append(";");
			break;
		}
		sb.append(json.get("base")).append(";");
		sb.append(json.getJSONObject("main").get("temp")).append(";");
		sb.append(json.getJSONObject("main").get("pressure")).append(";");
		sb.append(json.getJSONObject("main").get("humidity")).append(";");
		sb.append(json.getJSONObject("main").get("temp_min")).append(";");
		sb.append(json.getJSONObject("main").get("temp_max")).append(";");
		sb.append(json.get("visibility")).append(";");
		sb.append(json.getJSONObject("wind").get("speed")).append(";");
		sb.append(json.getJSONObject("wind").get("deg")).append(";");
		sb.append(json.getJSONObject("clouds").get("all")).append(";");
		sb.append(json.get("dt")).append(";");
		sb.append(json.getJSONObject("sys").get("type")).append(";");
		sb.append(json.getJSONObject("sys").get("id")).append(";");
		sb.append(json.getJSONObject("sys").get("country")).append(";");
		sb.append(json.getJSONObject("sys").get("sunrise")).append(";");
		sb.append(json.getJSONObject("sys").get("sunset")).append(";");
		sb.append(json.get("id")).append(";");
		sb.append(json.get("name")).append(";");
		sb.append(json.get("cod"));
		return sb.toString();
	}

	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
		InputStream is = new URL(url).openStream();
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
			return json;
		} finally {
			is.close();
		}
	}

	public Properties getProperties(String path) throws IOException {
		Properties properties = new Properties();
		properties.load(new FileReader(path));
		return properties;
	}
	
	public String loadProperties(String path,String key) throws FileNotFoundException, IOException {
		Properties properties = new Properties();
		properties.load(new FileReader(path));
		return properties.getProperty(key);
	}

	@Override
	public void run() {
		String[] cities=null;
		String appKey=null;
		int frequency=0;
		try {
			cities=loadProperties(propertiesFilePath,"locations").split(",");
			appKey=loadProperties(propertiesFilePath,"app_key");
			frequency=Integer.parseInt(loadProperties(propertiesFilePath, "frequency_sec").trim());
		}catch(IOException e2) {
			System.out.println("Unable to read config file");
			System.exit(0);
		}
		while(true) {
			try(BufferedWriter writer=new BufferedWriter(new FileWriter("input/"+Calendar.getInstance().getTimeInMillis()+".csv"))) {
				
				for(int i=0;i<(null!=cities?cities.length:0);i++) {
					try {
						JSONObject json=readJsonFromUrl("http://api.openweathermap.org/data/2.5/weather?q="+cities[i].trim()+"&appid="+appKey);
						BufferedWriter raw_writer=new BufferedWriter(new FileWriter("raw/"+Calendar.getInstance().getTimeInMillis()+"_"+cities[i]+".json"));
						raw_writer.write(json.toString());
						raw_writer.close();
						writer.write(processJSON2CSV(json)+"\r\n");
					}catch(Exception e) {
						System.out.println("unable find city in db: "+cities[i]);
					}
				}
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				TimeUnit.SECONDS.sleep(frequency);
			}catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
