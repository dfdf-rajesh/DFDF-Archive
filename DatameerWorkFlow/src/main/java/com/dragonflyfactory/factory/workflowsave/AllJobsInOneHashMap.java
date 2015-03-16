package com.dragonflyfactory.factory.workflowsave;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.json.simple.parser.JSONParser;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.dragonflyfactory.factory.workerwatchdog.watchdogs.DatameerWatchDog;
import com.dragonflyfactory.factory.workerwatchdog.watchdogs.IDatameerWatchDog;

public class AllJobsInOneHashMap {
	
	static Logger log = Logger.getLogger(AllJobsInOneHashMap.class.getName());
	
	public static void main(String args[])
	{
		giveDatameerObjectsASMap("admin", "dragon", "54.86.180.167", "7777");
	}
	
	public static HashMap giveDatameerObjectsASMap(String username,String password,String hostId,String portNo) {
		
		int noOfConnections=0,noOfImports=0,noOfWorkbooks=0,noOfExports=0;
		int noOfSystemConnections=0,noOfSystemImports=0,noOfSystemWorkbooks=0,noOfSystemExports=0;
		int noOfFailedToReadConnections=0,noOfFailedToReadImports=0,noOfFailedToReadWorkbooks=0,noOfFailedToReadExports=0;
		Long toBeDeletedConnectionId=Long.valueOf(0);
		
		try {
			

			IDatameerWatchDog datameerWatchDog = new DatameerWatchDog();
			JSONParser parser = new JSONParser();
			
			log.info("started AllJobsInOneHashMap");
			
			
			HashMap<String,Object> map = new HashMap<String,Object>();
			try {
				String allConnectionDetails = datameerWatchDog.readConnection("admin", "dragon", "54.86.180.167", "7777","");
				JSONArray connectionJSONArray =(JSONArray)parser.parse(allConnectionDetails);
				for(Object connection:connectionJSONArray) {
					JSONObject connectionJSON=(JSONObject)connection;
					Long connectionId=(Long)connectionJSON.get("id");
					String connectionPath=(String)connectionJSON.get("path");
					String connectionUuid=(String)connectionJSON.get("uuid");
					if (connectionPath.contains(".system")) {
						noOfSystemConnections++;
						continue;
					}
					if (connectionPath.contains("TwitterRESTCopy")) {
						
						log.info("connectionPath="+connectionPath);
						toBeDeletedConnectionId=connectionId;
					}

					String connectionFullDetails=datameerWatchDog.readConnection("admin", "dragon", "54.86.180.167", "7777",connectionId+"");
					if (connectionFullDetails!=null) {
						noOfConnections++;
						JSONObject connectionFullDetailsJSON=(JSONObject)parser.parse(connectionFullDetails);
						map.put(connectionPath+"-:-"+connectionUuid, connectionFullDetailsJSON);
						if (toBeDeletedConnectionId>0) {
							datameerWatchDog.deleteConnection("admin", "dragon", "54.86.180.167", "7777",toBeDeletedConnectionId+"");
							return map;
						}
					}
					else {
						noOfFailedToReadConnections++;
					}
				}
				String allImportJobDetails = datameerWatchDog.readImportJob("admin", "dragon", "54.86.180.167", "7777","");
				String allWorkbookDetails = datameerWatchDog.readWorkBookJob("admin", "dragon", "54.86.180.167", "7777","");
				String allExportJobDetails = datameerWatchDog.readExportJob("admin", "dragon", "54.86.180.167", "7777","");

				JSONArray importJobDetailsJSONArray =(JSONArray)parser.parse(allImportJobDetails);
				for(Object importJobDetails:importJobDetailsJSONArray) {
					JSONObject importJobDetailsJSON=(JSONObject)importJobDetails;
					Long importJobId=(Long)importJobDetailsJSON.get("id");
					String importJobPath=(String)importJobDetailsJSON.get("path");
					String importJobUuid=(String)importJobDetailsJSON.get("uuid");
					if (importJobPath.contains(".system")) {
						noOfSystemImports++;
						continue;
					}
					String importJobFullDetails=datameerWatchDog.readImportJob("admin", "dragon", "54.86.180.167", "7777",importJobId+"");
					if (importJobFullDetails!=null) {
						noOfImports++;
						JSONObject importJobFullDetailsJSON=(JSONObject)parser.parse(importJobFullDetails);
						map.put(importJobPath+"-:-"+importJobUuid, importJobFullDetailsJSON);
					}
					else {
						noOfFailedToReadImports++;
					}

				}
				JSONArray workbookDetailsJSONArray =(JSONArray)parser.parse(allWorkbookDetails);
				for(Object workbook:workbookDetailsJSONArray) {
					JSONObject workbookJSON=(JSONObject)workbook;
					Long workbookId=(Long)workbookJSON.get("id");
					String workbookPath=(String)workbookJSON.get("path");
					String workbookUuid=(String)workbookJSON.get("uuid");
					if (workbookPath.contains(".system")) {
						noOfSystemWorkbooks++;
						continue;
					}
					String workbookFullDetails=datameerWatchDog.readWorkBookJob("admin", "dragon", "54.86.180.167", "7777",workbookId+"");
					if (workbookFullDetails!=null) {
						noOfWorkbooks++;
						JSONObject workbookFullDetailsJSON=(JSONObject)parser.parse(workbookFullDetails);
						map.put(workbookPath+"-:-"+workbookUuid, workbookFullDetailsJSON);
					}
					else {
						noOfFailedToReadWorkbooks++;
					}

				}
				JSONArray exportJobDetailsJSONArray =(JSONArray)parser.parse(allExportJobDetails);
				for(Object exportJobDetails:exportJobDetailsJSONArray) {
					JSONObject exportJobDetailsJSON=(JSONObject)exportJobDetails;
					Long exportJobId=(Long)exportJobDetailsJSON.get("id");
					String exportJobPath=(String)exportJobDetailsJSON.get("path");
					String exportJobUuid=(String)exportJobDetailsJSON.get("uuid");
					if (exportJobPath.contains(".system")) {
						noOfSystemExports++;
						continue;
					}
					String exportJobFullDetails=datameerWatchDog.readExportJob("admin", "dragon", "54.86.180.167", "7777",exportJobId+"");
					if (exportJobFullDetails!=null) { 
						noOfExports++;
						JSONObject exportJobFullDetailsJSON=(JSONObject)parser.parse(exportJobFullDetails);
						map.put(exportJobPath+"-:-"+exportJobUuid, exportJobFullDetailsJSON);
					}
					else {
						noOfFailedToReadExports++;
					}

				}
				
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			int totalNoOfJobs=map.size();
			log.info("totalNoOfJobs in map = "+totalNoOfJobs);
			log.info("noOfConnections="+noOfConnections);
			log.info("noOfImports="+noOfImports);
			log.info("noOfWorkbooks="+noOfWorkbooks);
			log.info("noOfExports="+noOfExports);
			log.info("noOfFailedToReadConnections="+noOfFailedToReadConnections);
			log.info("noOfFailedToReadImports="+noOfFailedToReadImports);
			log.info("noOfFailedToReadWorkbooks="+noOfFailedToReadWorkbooks);
			log.info("noOfFailedToReadExports="+noOfFailedToReadExports);
			log.info("noOfSystemConnections="+noOfSystemConnections);
			log.info("noOfSystemImports="+noOfSystemImports);
			log.info("noOfSystemWorkbooks="+noOfSystemWorkbooks);
			log.info("noOfSystemExports="+noOfSystemExports);
			log.info("ending AllJobsInOneHashMap");
			
			return map;
		
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		return null;
	}
	
	public static void createGivenDatameerObjectsASMapIntoDataMeerJobs(HashMap map,String username,String password,String hostId,String portNo) {
		
		try {
			IDatameerWatchDog datameerWatchDog = new DatameerWatchDog();
			JSONParser parser = new JSONParser();
			
			if (map==null) {
				return;
			}
			Set keySet=map.keySet();
			
			int i=0;
			for(Object key:keySet) {
				
				String keyString = (String)key;
				//i++;
				//if (i==2) {
				if (keyString.contains("TwitterRESTCopy")) {
				
					JSONObject connectionJSON=(JSONObject)map.get(key);
					log.info("connectionJSON = "+connectionJSON.toString());
					if (keyString.contains(".dst")) {
//						JSONObject file= (JSONObject)connectionJSON.get("file");
//						String path= (String)file.get("path");
//						String newPath=path.substring(0, path.length()-4)+"Copy"+".dst";
//						file.put("path", newPath);
//						connectionJSON.put("file", file);
						
						datameerWatchDog.createConnection("admin", "dragon",connectionJSON.toString() ,"54.86.180.167", "7777");
					}
				}
				
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}

}
