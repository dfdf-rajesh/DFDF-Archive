package com.dragonflyfactory.factory.workflowctrl;

import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class JSONPayLoad {
	
	public static String createMySqlConnectionPayLoad(String connectionName,String path,String connectionUrl,String dataStoreUsage,
							String username,String password,String dbType) {
		
		HashMap map=new HashMap();
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader("C:/Test/mysql.json"));
			JSONObject jsonObj =(JSONObject) obj;
			System.out.println("jsonObj="+jsonObj);
			JSONObject properties =(JSONObject)jsonObj.get("properties");
			System.out.println("properties="+properties);
			JSONObject file =(JSONObject)jsonObj.get("file");
			System.out.println("file="+file);
			if (path!=null && !path.equals(""))
			{
				file.put("path", path+"/"+connectionName+".dst");
			}
			else
			{
				file.put("path", "/Data/Connections/"+connectionName+".dst");
			}
			file.put("description",connectionName+"_Details");
			file.put("name",connectionName);
			List<String> listusername=new ArrayList<String>();
			listusername.add(username);
			properties.put("user_name", listusername);
			List<String> listpassword=new ArrayList<String>();
			listpassword.add(password);
			List<String> listconnectionUrl=new ArrayList<String>();
			listconnectionUrl.add(connectionUrl);
			properties.put("password",listpassword );
			properties.put("key.connectionUrl",listconnectionUrl );
			List<String> listdataStoreUsage=new ArrayList<String>();
			listdataStoreUsage.add("IMPORT_EXPORT");
			properties.put("dataStoreUsage", listdataStoreUsage);
			HashMap dbTypeMap=new HashMap();
			dbTypeMap.put("name", dbType);
			jsonObj.put("dbType", dbTypeMap);
			System.out.println("jsonObj="+jsonObj);
			System.out.println("file="+file);
			System.out.println("properties="+properties);
			jsonObj.put("properties", properties);
			jsonObj.put("file", file);
			System.out.println("jsonObj="+jsonObj);
			String fileName="C:/Test/"+connectionName+"Connection.json";
			FileWriter file2 = new FileWriter(fileName);
			file2.write(jsonObj.toJSONString());
			file2.flush();
			file2.close();
			
		
		System.out.println("jsonObj="+jsonObj);
		return fileName;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return null;
	}

	public static String createHiveConnectionPayLoad(String connectionName,String metastore_uri,String dataStoreUsage) {

		HashMap map=new HashMap();
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader("C:/Test/sampleHiveConnectionPayLoad.json"));
			JSONObject jsonObj =(JSONObject) obj;
			System.out.println("jsonObj="+jsonObj);
			JSONObject properties =(JSONObject)jsonObj.get("properties");
			System.out.println("properties="+properties);
			JSONObject file =(JSONObject)jsonObj.get("file");
			System.out.println("file="+file);
			file.put("path", "/Data/Connections/"+connectionName+".dst");
			file.put("description",connectionName+"_Details");
			file.put("name",connectionName);
			List<String> metastoreUriList=new ArrayList<String>();
			metastoreUriList.add(metastore_uri);
			properties.put("metastore_uri",metastoreUriList );
			List<String> listdataStoreUsage=new ArrayList<String>();
			listdataStoreUsage.add(dataStoreUsage);
			properties.put("dataStoreUsage", listdataStoreUsage);
			System.out.println("jsonObj="+jsonObj);
			System.out.println("file="+file);
			System.out.println("properties="+properties);
			jsonObj.put("properties", properties);
			jsonObj.put("file", file);
			System.out.println("jsonObj="+jsonObj);
			String fileName="C:/Test/"+connectionName+"Connection.json";
			FileWriter file2 = new FileWriter(fileName);
			file2.write(jsonObj.toJSONString());
			file2.flush();
			file2.close();


			System.out.println("jsonObj="+jsonObj);
			return fileName;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		return null;
	}

	public static String createHDFSConnectionPayLoad(String connectionName,String dataStoreUsage,
			String host,String port,String rootPathPrefix) {

		
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader("C:/Test/hdfsconnection.json"));
			JSONObject jsonObj =(JSONObject) obj;
			System.out.println("jsonObj="+jsonObj);
			JSONObject properties =(JSONObject)jsonObj.get("properties");
			System.out.println("properties="+properties);
			JSONObject file =(JSONObject)jsonObj.get("file");
			System.out.println("file="+file);
			file.put("path", "/Data/Connections/"+connectionName+".dst");
			file.put("description",connectionName+"_Details");
			file.put("name",connectionName);
			List<String> listhost=new ArrayList<String>();
			listhost.add(host);
			properties.put("host", listhost);
			List<String> listport=new ArrayList<String>();
			listport.add(port);
			properties.put("port",listport );
			List<String> listdataStoreUsage=new ArrayList<String>();
			listdataStoreUsage.add("IMPORT_EXPORT");
			properties.put("dataStoreUsage", listdataStoreUsage);
			List<String> listrootPathPrefix=new ArrayList<String>();;
			listrootPathPrefix.add( rootPathPrefix);
			properties.put("rootPathPrefix", listrootPathPrefix);
			System.out.println("jsonObj="+jsonObj);
			System.out.println("file="+file);
			System.out.println("properties="+properties);
			jsonObj.put("properties", properties);
			jsonObj.put("file", file);
			System.out.println("jsonObj="+jsonObj);
			String fileName="C:/Test/"+connectionName+".json";
			FileWriter file2 = new FileWriter(fileName);
			file2.write(jsonObj.toJSONString());
			file2.flush();
			file2.close();


			System.out.println("jsonObj="+jsonObj);
			return fileName;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		return null;
	}



	public static String createCustomPlugginConnectionPayLoad(String connectionName,String typeId) {

		HashMap map=new HashMap();
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader("C:/Test/sampleCustomPlugginConnection.json"));
			JSONObject jsonObj =(JSONObject) obj;
			System.out.println("jsonObj="+jsonObj);
			JSONObject properties =(JSONObject)jsonObj.get("properties");
			System.out.println("properties="+properties);
			JSONObject file =(JSONObject)jsonObj.get("file");
			System.out.println("file="+file);
			file.put("path", "/Data/Connections/"+connectionName+".dst");
			file.put("description",connectionName+"_Details");
			file.put("name",connectionName);
			List<String> listdataStoreUsage=new ArrayList<String>();
			listdataStoreUsage.add("IMPORT");
			properties.put("dataStoreUsage", listdataStoreUsage);
			System.out.println("jsonObj="+jsonObj);
			System.out.println("file="+file);
			System.out.println("properties="+properties);
			jsonObj.put("properties", properties);
			jsonObj.put("file", file);
			jsonObj.put("typeId", typeId);
			System.out.println("jsonObj="+jsonObj);
			String fileName="C:/Test/"+connectionName+".json";
			FileWriter file2 = new FileWriter(fileName);
			file2.write(jsonObj.toJSONString());
			file2.flush();
			file2.close();


			System.out.println("jsonObj="+jsonObj);
			return fileName;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		return null;
	}
	
	public static String createMySqlImportJobPayLoad(String importJobName,String path,String databaseName,String connectionUrl,String username,
			String password,String tableName,String connectionNamePath,String uuid) {
		
		
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader("C:/Test/sampleMySqlImportPayLoad.json"));
			JSONObject jsonObj =(JSONObject) obj;
			System.out.println("jsonObj="+jsonObj);
			JSONObject dataStore =(JSONObject)jsonObj.get("dataStore");
			System.out.println("dataStore="+dataStore);
			JSONObject file =(JSONObject)jsonObj.get("file");
			System.out.println("file="+file);
			//file.put("path", "/Data/ImportJobs/usecase4/"+importJobName+".imp");
			if (path!=null && !path.equals(""))
			{
				file.put("path", path+"/"+importJobName+".imp");
			}
			else
			{
				file.put("path", "/Data/ImportJobs/"+importJobName+".imp");
			}
			
			file.put("description",importJobName+"_Details");
			file.put("name",importJobName);
			
			
			dataStore.put("path", connectionNamePath);
			dataStore.put("uuid", uuid);
			System.out.println("jsonObj="+jsonObj);
			System.out.println("file="+file);
			System.out.println("dataStore="+dataStore);
			jsonObj.put("dataStore", dataStore);
			jsonObj.put("file", file);
			JSONObject properties=(JSONObject)jsonObj.get("properties");
			ArrayList tableList=new ArrayList();
			tableList.add("\u0027"+tableName+"\u0027");
			properties.put("jdbc.property.table",tableList);
			jsonObj.put("properties", properties);
			List fields=new ArrayList();
			Connection conn = getMySqlConnection(connectionUrl, username, password);
			
			Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
			        ResultSet.CONCUR_UPDATABLE);
			
			ResultSet rs = st.executeQuery("SELECT * FROM "+tableName+" limit 1 ");
			
			ResultSetMetaData rsMetaData = rs.getMetaData();
			int numberOfColumns = rsMetaData.getColumnCount();
			int id=2000;
			for (int i = 1; i < numberOfColumns + 1; i++) {
			      String columnName = rsMetaData.getColumnName(i);
			      
			      HashMap<String,Object> fieldObject=new HashMap<String,Object>();
			      fieldObject.put("id", id++);
			      fieldObject.put("pattern", "");
			      fieldObject.put("acceptEmpty", rsMetaData.isNullable(i)==1 ? true : false);
			      fieldObject.put("name", columnName);
			      fieldObject.put("origin", columnName);
			      String type=rsMetaData.getColumnTypeName(i);
			      System.out.println("type="+type);
			      if (type.equalsIgnoreCase("VARCHAR")||type.equalsIgnoreCase("CHAR")) {
			    	  type="STRING";
			      }
			      else if  (type.equalsIgnoreCase("DOUBLE")) {
			    	  type="FLOAT";
			      }
			      else if  (type.equalsIgnoreCase("BIGINT")) {
			    	  type="INTEGER";
			      }
			      else if  (type.contains("INTEGER")) {
			    	  type="INTEGER";
			      }
			      else if  (type.equalsIgnoreCase("DATETIME")) {
			    	  type="DATE";
			      }
			      fieldObject.put("valueType", "{\"type\":\""+type+"\"}");
			      fieldObject.put("include", true);
			      fieldObject.put("version", 3);
			      fields.add(fieldObject);
			      
			      System.out.println(columnName+" is null accepted = "+rsMetaData.isNullable(i)+" and its type is "+rsMetaData.getColumnTypeName(i));
			    }
		    rs.close();
		    st.close();
		    conn.close();
		    jsonObj.put("fields", fields);
			System.out.println("jsonObj="+jsonObj);
			String fileName="C:/Test/"+importJobName+"ImportPayLoad.json";
			FileWriter file2 = new FileWriter(fileName);
			file2.write(jsonObj.toJSONString());
			file2.flush();
			file2.close();
			
		
		System.out.println("jsonObj="+jsonObj);
		return fileName;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return null;
	}
	


	public static String createCustomPlugginImportJobPayLoad(String importJobName,String noOfFiles,String noOfRecords,String connectionNamePath,String uuid) {
		
		
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader("C:/Test/sampleCustomPlugginImportPayLoad.json"));
			JSONObject jsonObj =(JSONObject) obj;
			System.out.println("jsonObj="+jsonObj);
			JSONObject dataStore =(JSONObject)jsonObj.get("dataStore");
			System.out.println("dataStore="+dataStore);
			JSONObject file =(JSONObject)jsonObj.get("file");
			System.out.println("file="+file);
			file.put("path", "/Data/ImportJobs/"+importJobName+".imp");
			file.put("description",importJobName+"_Details");
			file.put("name",importJobName);
			
			
			dataStore.put("path", connectionNamePath);
			dataStore.put("uuid", uuid);
			System.out.println("jsonObj="+jsonObj);
			System.out.println("file="+file);
			System.out.println("dataStore="+dataStore);
			jsonObj.put("dataStore", dataStore);
			jsonObj.put("file", file);
			//"files-number"
			//"file-records"
			ArrayList<String> numeberOfFiles=new ArrayList<String>();
			numeberOfFiles.add(noOfFiles);
			ArrayList<String> numeberOfRecords=new ArrayList<String>();
			numeberOfRecords.add(noOfRecords);
			JSONObject properties =(JSONObject)jsonObj.get("properties");
			jsonObj.put("files-number", numeberOfFiles);
			jsonObj.put("file-records", numeberOfRecords);
			properties.put("files-number", numeberOfFiles);
			properties.put("file-records", numeberOfRecords);
			jsonObj.put("properties", properties);
			System.out.println("jsonObj="+jsonObj);
			String fileName="C:/Test/"+importJobName+".json";
			FileWriter file2 = new FileWriter(fileName);
			file2.write(jsonObj.toJSONString());
			file2.flush();
			file2.close();
			
		
		System.out.println("jsonObj="+jsonObj);
		return fileName;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return null;
	}

	public static String createWorkBookJobPayLoad(String workBookJobName,String path,String importJobPath,String importJob_uuid,String importJobNameOrOriginalName) {
		
		
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader("C:/Test/sampleWorkBookPayLoad.json"));
			JSONObject jsonObj =(JSONObject) obj;
			System.out.println("jsonObj="+jsonObj);
			JSONArray sheetsList =(JSONArray)jsonObj.get("sheets");
			int sheetsListSize=sheetsList.size();
			JSONObject datasource =null;
			int botNameAndDatasourceFound=0;
			for(int ii=0;ii<sheetsListSize;ii++)
			{
				JSONObject json=(JSONObject) sheetsList.get(ii);
				System.out.println("in loop json="+json);
				if (json.get("datasource")!=null) {
					datasource=(JSONObject) json.get("datasource");
					botNameAndDatasourceFound++;
					datasource.put("path", importJobPath);
					datasource.put("uuid", importJob_uuid);
					if (botNameAndDatasourceFound==2) 
					{
						break;
					}
				}
				if (json.get("name")!=null) {
					
					json.put("name", importJobNameOrOriginalName);
					botNameAndDatasourceFound++;
					if (botNameAndDatasourceFound==2) 
					{
						break;
					}
				}
			}
			System.out.println("datasource="+datasource);
			JSONObject file =(JSONObject)jsonObj.get("file");
			System.out.println("file="+file);
			//file.put("path", "/Analytics/Workbooks/usecase4/"+workBookJobName+".wbk");
			if (path!=null && !path.equals(""))
			{
				file.put("path", path+"/"+workBookJobName+".wbk");
			}
			else
			{
				file.put("path", "/Analytics/Workbooks/"+workBookJobName+".wbk");
			}
			
			file.put("description",workBookJobName+"_Details");
			file.put("name",workBookJobName);
			
			
			System.out.println("jsonObj="+jsonObj);
			System.out.println("file="+file);
			System.out.println("datasource="+datasource);
			jsonObj.put("file", file);
			jsonObj.put("sheets", sheetsList);
			
			System.out.println("jsonObj="+jsonObj);
			String fileName="C:/Test/"+workBookJobName+".json";
			FileWriter file2 = new FileWriter(fileName);
			file2.write(jsonObj.toJSONString());
			file2.flush();
			file2.close();
			
		
		System.out.println("jsonObj="+jsonObj);
		return fileName;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return null;
	}
	


	public static String createExportJobPayLoad(String exportJobName,String connectionPath,String workbookPath,String workbook_uuid,String workbookJobNameOrOriginalName) {
		
		
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader("C:/Test/sampleExportPayLoad.json"));
			JSONObject jsonObj =(JSONObject) obj;
			System.out.println("jsonObj="+jsonObj);
			JSONObject sheet =(JSONObject)jsonObj.get("sheet");
			JSONObject workbook =(JSONObject) sheet.get("workbook");
			workbook.put("path", workbookPath);
			workbook.put("uuid", workbook_uuid);
			sheet.put("name", workbookJobNameOrOriginalName);
			
			System.out.println("workbook="+workbook);
			JSONObject file =(JSONObject)jsonObj.get("file");
			System.out.println("file="+file);
			file.put("path", "/Data/Exports/"+exportJobName+".exp");
			file.put("description",exportJobName+"_Details");
			file.put("name",exportJobName);
			
			
			System.out.println("jsonObj="+jsonObj);
			System.out.println("file="+file);
			jsonObj.put("file", file);
			jsonObj.put("sheet", sheet);
			
			System.out.println("jsonObj="+jsonObj);
			String fileName="C:/Test/"+exportJobName+".json";
			FileWriter file2 = new FileWriter(fileName);
			file2.write(jsonObj.toJSONString());
			file2.flush();
			file2.close();
			
		
		System.out.println("jsonObj="+jsonObj);
		return fileName;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return null;
	}

	public static String createExportJobPayLoadUsingHDFSConnection(String exportJobName,String connectionPath,String workbookPath,String workbook_uuid,String importJobNameOrOriginalName,String outputFileName) {
		
		
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader("C:/Test/sampleExportPayLoadWithHDFSConnection.json"));
			JSONObject jsonObj =(JSONObject) obj;
			System.out.println("jsonObj="+jsonObj);
			JSONObject sheet =(JSONObject)jsonObj.get("sheet");
			JSONObject workbook =(JSONObject) sheet.get("workbook");
			workbook.put("path", workbookPath);
			workbook.put("uuid", workbook_uuid);
			sheet.put("name", importJobNameOrOriginalName);
			
			System.out.println("workbook="+workbook);
			JSONObject file =(JSONObject)jsonObj.get("file");
			System.out.println("file="+file);
			file.put("path", "/Data/Exports/"+exportJobName+".exp");
			file.put("description",exportJobName+"_Details");
			file.put("name",exportJobName);
			
			//JSONObject outputFile =(JSONObject)jsonObj.get("fileName");
			//outputFile.put("fileName", outputFileName);
			jsonObj.put("fileName", outputFileName);
			
			System.out.println("jsonObj="+jsonObj);
			System.out.println("file="+file);
			jsonObj.put("file", file);
			jsonObj.put("sheet", sheet);
			
			System.out.println("jsonObj="+jsonObj);
			String fileName="C:/Test/"+exportJobName+".json";
			FileWriter file2 = new FileWriter(fileName);
			file2.write(jsonObj.toJSONString());
			file2.flush();
			file2.close();
			
		
		System.out.println("jsonObj="+jsonObj);
		return fileName;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return null;
	}

	public static String createExportJobPayLoadForCustomPlugginUsingHDFSConnection(String exportJobName,String connectionPath,String workbookPath,String workbook_uuid,String importJobNameOrOriginalName,String outputFileName) {
		
		
		JSONParser parser = new JSONParser();
		try {
			Object obj = parser.parse(new FileReader("C:/Test/sampleExportPayLoadWithCustomPluggin.json"));
			JSONObject jsonObj =(JSONObject) obj;
			System.out.println("jsonObj="+jsonObj);
			JSONObject sheet =(JSONObject)jsonObj.get("sheet");
			JSONObject workbook =(JSONObject) sheet.get("workbook");
			workbook.put("path", workbookPath);
			workbook.put("uuid", workbook_uuid);
			sheet.put("name", importJobNameOrOriginalName);
			
			System.out.println("workbook="+workbook);
			JSONObject file =(JSONObject)jsonObj.get("file");
			System.out.println("file="+file);
			file.put("path", "/Data/Exports/"+exportJobName+".exp");
			file.put("description",exportJobName+"_Details");
			file.put("name",exportJobName);
			
			//JSONObject outputFile =(JSONObject)jsonObj.get("fileName");
			//outputFile.put("fileName", outputFileName);
			jsonObj.put("fileName", outputFileName);
			
			System.out.println("jsonObj="+jsonObj);
			System.out.println("file="+file);
			jsonObj.put("file", file);
			jsonObj.put("sheet", sheet);
			
			System.out.println("jsonObj="+jsonObj);
			String fileName="C:/Test/"+exportJobName+".json";
			FileWriter file2 = new FileWriter(fileName);
			file2.write(jsonObj.toJSONString());
			file2.flush();
			file2.close();
			
		
		System.out.println("jsonObj="+jsonObj);
		return fileName;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return null;
	}

	private static Connection getMySqlConnection(String connectionUrl,String username,String password) throws Exception {
	   
		//Class.forName("org.hsqldb.jdbcDriver");
	   
		 Class.forName("com.mysql.jdbc.Driver");
	    return DriverManager.getConnection(connectionUrl, username, password);
	  }

}

