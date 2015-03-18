package com.dragonflyfactory.factory.workflowsave;

import java.util.ArrayList;
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

	public static void main(String args[]) {
		giveDatameerObjectsASMap("Afzal", "admin", "54.86.180.167", "7777");
	}

	public static HashMap giveDatameerObjectsASMap(String username,
			String password, String hostId, String portNo) {

		int noOfConnections = 0, noOfImports = 0, noOfWorkbooks = 0, noOfExports = 0;
		int noOfDeletedConnections = 0, noOfDeletedImports = 0, noOfDeletedWorkbooks = 0, noOfDeletedExports = 0;
		int noOfBackedUpConnections = 0, noOfBackedUpImports = 0, noOfBackedUpWorkbooks = 0, noOfBackedUpExports = 0;
		int noOfSystemConnections = 0, noOfSystemImports = 0, noOfSystemWorkbooks = 0, noOfSystemExports = 0;
		int noOfFailedToReadConnections = 0, noOfFailedToReadImports = 0, noOfFailedToReadWorkbooks = 0, noOfFailedToReadExports = 0;
		

		try {

			IDatameerWatchDog datameerWatchDog = new DatameerWatchDog();
			JSONParser parser = new JSONParser();

			log.info("started AllJobsInOneHashMap");

			HashMap<String, Object> map = new HashMap<String, Object>();
			try {
				String allConnectionDetails = datameerWatchDog.readConnection(
						username, password, hostId, portNo, "");
				String allImportJobDetails = datameerWatchDog.readImportJob(
						username, password, hostId, portNo, "");
				String allWorkbookDetails = datameerWatchDog.readWorkBookJob(
						username, password, hostId, portNo, "");
				String allExportJobDetails = datameerWatchDog.readExportJob(
						username, password, hostId, portNo, "");
				if (allExportJobDetails != null) {
					JSONArray exportJobDetailsJSONArray = (JSONArray) parser
							.parse(allExportJobDetails);
					if (exportJobDetailsJSONArray != null) {
						for (Object exportJobDetails : exportJobDetailsJSONArray) {
							JSONObject exportJobDetailsJSON = (JSONObject) exportJobDetails;
							Long exportJobId = (Long) exportJobDetailsJSON
									.get("id");
							String exportJobPath = (String) exportJobDetailsJSON
									.get("path");
							String exportJobUuid = (String) exportJobDetailsJSON
									.get("uuid");
							if (exportJobPath!=null && exportJobPath.contains(".system")) {
								noOfSystemExports++;
								continue;
							}
							String exportJobFullDetails = datameerWatchDog
									.readExportJob(username, password, hostId,
											portNo, exportJobId + "");
							if (exportJobFullDetails != null) {
								noOfExports++;
								JSONObject exportJobFullDetailsJSON = (JSONObject) parser
										.parse(exportJobFullDetails);

								String result = datameerWatchDog.deleteExportJob(username,password, hostId,portNo,exportJobId+"");
								if (result!=null && result.contains("success")) {
									
									noOfDeletedExports++;

									map.put(exportJobPath + "-:-" + exportJobUuid,
											exportJobFullDetailsJSON);
									noOfBackedUpExports++;
									
								}
								
							} else {
								noOfFailedToReadExports++;
							}

						}
					}
				}

				if (allWorkbookDetails != null) {
					JSONArray workbookDetailsJSONArray = (JSONArray) parser
							.parse(allWorkbookDetails);
					if (workbookDetailsJSONArray != null) {
						for (Object workbook : workbookDetailsJSONArray) {
							JSONObject workbookJSON = (JSONObject) workbook;
							Long workbookId = (Long) workbookJSON.get("id");
							String workbookPath = (String) workbookJSON
									.get("path");
							String workbookUuid = (String) workbookJSON
									.get("uuid");
							if (workbookPath!=null && workbookPath.contains(".system")) {
								noOfSystemWorkbooks++;
								continue;
							}
							String workbookFullDetails = datameerWatchDog
									.readWorkBookJob(username, password,
											hostId, portNo, workbookId + "");
							if (workbookFullDetails != null) {
								noOfWorkbooks++;
								JSONObject workbookFullDetailsJSON = (JSONObject) parser
										.parse(workbookFullDetails);

								String result = datameerWatchDog.deleteWorkBookJob(username,password, hostId,portNo,workbookId+"");
								if (result!=null && result.contains("success")) {
									
									noOfDeletedWorkbooks++;

									map.put(workbookPath + "-:-" + workbookUuid,
											workbookFullDetailsJSON);
									noOfBackedUpWorkbooks++;
									
								}
								
							} else {
								noOfFailedToReadWorkbooks++;
							}

						}
					}
				}

				if (allImportJobDetails != null) {
					JSONArray importJobDetailsJSONArray = (JSONArray) parser
							.parse(allImportJobDetails);
					if (importJobDetailsJSONArray != null) {
						for (Object importJobDetails : importJobDetailsJSONArray) {
							JSONObject importJobDetailsJSON = (JSONObject) importJobDetails;
							Long importJobId = (Long) importJobDetailsJSON
									.get("id");
							String importJobPath = (String) importJobDetailsJSON
									.get("path");
							String importJobUuid = (String) importJobDetailsJSON
									.get("uuid");
							if (importJobPath!=null && importJobPath.contains(".system")) {
								noOfSystemImports++;
								continue;
							}
							String importJobFullDetails = datameerWatchDog
									.readImportJob(username, password, hostId,
											portNo, importJobId + "");
							if (importJobFullDetails != null) {
								noOfImports++;
								JSONObject importJobFullDetailsJSON = (JSONObject) parser
										.parse(importJobFullDetails);

								String result = datameerWatchDog.deleteImportJob(username,password, hostId,portNo,importJobId+"");
								if (result!=null && result.contains("success")) {
									
									noOfDeletedImports++;

									map.put(importJobPath + "-:-" + importJobUuid,
											importJobFullDetailsJSON);
									noOfBackedUpImports++;
									
								}
								
								
							} else {
								noOfFailedToReadImports++;
							}

						}
					}
				}

				
				if (allConnectionDetails != null) {
					JSONArray connectionJSONArray = (JSONArray) parser
							.parse(allConnectionDetails);

					if (connectionJSONArray != null) {
						for (Object connection : connectionJSONArray) {
							JSONObject connectionJSON = (JSONObject) connection;
							Long connectionId = (Long) connectionJSON.get("id");
							String connectionPath = (String) connectionJSON
									.get("path");
							String connectionUuid = (String) connectionJSON
									.get("uuid");
							if (connectionPath!=null && connectionPath.contains(".system")) {
								noOfSystemConnections++;
								continue;
							}
							// if (connectionPath.contains("TwitterRESTCopy")) {
							//
							// log.info("connectionPath="+connectionPath);
							// toBeDeletedConnectionId=connectionId;
							// }

							String connectionFullDetails = datameerWatchDog
									.readConnection(username, password, hostId,
											portNo, connectionId + "");
							if (connectionFullDetails != null) {
								noOfConnections++;
								JSONObject connectionFullDetailsJSON = (JSONObject) parser
										.parse(connectionFullDetails);
								String result = datameerWatchDog.deleteConnection(username,password, hostId,portNo,connectionId+"");
								if (result!=null && result.contains("success")) {
									
									noOfDeletedConnections++;

									map.put(connectionPath + "-:-"
											+ connectionUuid,
											connectionFullDetailsJSON);
									noOfBackedUpConnections++;
									
								}
								// if (toBeDeletedConnectionId>0) {
								// datameerWatchDog.deleteConnection(username,
								// password, hostId,
								// portNo,toBeDeletedConnectionId+"");
								// return map;
								// }
							} else {
								noOfFailedToReadConnections++;
							}
						}
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
			int totalNoOfJobs = map.size();
			log.info("totalNoOfJobs in map = " + totalNoOfJobs);
			log.info("noOfConnections=" + noOfConnections);
			log.info("noOfImports=" + noOfImports);
			log.info("noOfWorkbooks=" + noOfWorkbooks);
			log.info("noOfExports=" + noOfExports);
			log.info("noOfDeletedConnections=" + noOfDeletedConnections);
			log.info("noOfDeletedImports=" + noOfDeletedImports);
			log.info("noOfDeletedWorkbooks=" + noOfDeletedWorkbooks);
			log.info("noOfDeletedExports=" + noOfDeletedExports);
			log.info("noOfBackedUpConnections=" + noOfBackedUpConnections);
			log.info("noOfBackedUpImports=" + noOfBackedUpImports);
			log.info("noOfBackedUpWorkbooks=" + noOfBackedUpWorkbooks);
			log.info("noOfBackedUpExports=" + noOfBackedUpExports);

			log.info("noOfFailedToReadConnections="
					+ noOfFailedToReadConnections);
			log.info("noOfFailedToReadImports=" + noOfFailedToReadImports);
			log.info("noOfFailedToReadWorkbooks=" + noOfFailedToReadWorkbooks);
			log.info("noOfFailedToReadExports=" + noOfFailedToReadExports);
			log.info("noOfSystemConnections=" + noOfSystemConnections);
			log.info("noOfSystemImports=" + noOfSystemImports);
			log.info("noOfSystemWorkbooks=" + noOfSystemWorkbooks);
			log.info("noOfSystemExports=" + noOfSystemExports);
			log.info("ending AllJobsInOneHashMap");

			return map;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public static void createGivenDatameerObjectsASMapIntoDataMeerJobs(
			HashMap map, String username, String password, String hostId,
			String portNo) {

		try {
			IDatameerWatchDog datameerWatchDog = new DatameerWatchDog();
			JSONParser parser = new JSONParser();

			if (map == null) {
				return;
			}
			Set keySet = map.keySet();

			//int i = 0;
			
			HashMap<String,Object> connectionHashMap = new HashMap<String,Object>();
			HashMap<String,Object> importHashMap = new HashMap<String,Object>();
			HashMap<String,Object> workbookHashMap = new HashMap<String,Object>();
			HashMap<String,String> sheetHashMap = new HashMap<String,String>();
			ArrayList<String> AllJobsList = new ArrayList<String>();
			ArrayList<String> connectionsList = new ArrayList<String>();
			ArrayList<String> importJobsList = new ArrayList<String>();
			ArrayList<String> workbookJobsList = new ArrayList<String>();
			ArrayList<String> exportJobsList = new ArrayList<String>();
			

			if (keySet != null && !keySet.isEmpty()) {
				
				for (Object key : keySet) {
					String keyString = (String) key;
					if (keyString.contains(".dst")) {
						connectionsList.add(keyString);
					}
					if (keyString.contains(".imp")) {
						importJobsList.add(keyString);
					}
					if (keyString.contains(".wbk")) {
						workbookJobsList.add(keyString);
					}
					if (keyString.contains(".exp")) {
						exportJobsList.add(keyString);
					}
					
				}
				AllJobsList.addAll(connectionsList);
				AllJobsList.addAll(importJobsList);
				AllJobsList.addAll(workbookJobsList);
				AllJobsList.addAll(exportJobsList);
				for (String key : AllJobsList) {

					
					// i++;
					// if (i==2) {
					//if (keyString.contains("TwitterRESTCopy")) {

						JSONObject json = (JSONObject) map.get(key);
						log.info("json = "
								+ json.toString());
						if (key.contains(".dst")) {
							// JSONObject file=
							// (JSONObject)json.get("file");
							// String path= (String)file.get("path");
							// String newPath=path.substring(0,
							// path.length()-4)+"Copy"+".dst";
							// file.put("path", newPath);
							// json.put("file", file);

							String result=datameerWatchDog.createConnection(username,
									password, json.toString(),
									hostId, portNo);
							if (result.contains("success")) {
								JSONObject connectionJSON = (JSONObject) parser
										.parse(result);
								Long connectionId = (Long) connectionJSON.get("configuration-id");
								String connectionFullDetails = datameerWatchDog
										.readConnection(username, password, hostId,
												portNo, connectionId + "");
								if (connectionFullDetails!=null)
								{
									JSONObject connectionFullDetailsJSON = (JSONObject) parser
											.parse(connectionFullDetails);
									 JSONObject file = (JSONObject)connectionFullDetailsJSON.get("file");
									 String path= (String)file.get("path");
									 connectionHashMap.put(path, file);
									
								}
								
							}
							else 
							{
								//throw new RuntimeException(" connection "+key+" not created successfully ");
							}

						}
						if (key.contains(".imp")) {

							 JSONObject dataStore = (JSONObject)json.get("dataStore");
							 String connectionPath= (String)dataStore.get("path");
							 JSONObject connectionFile =(JSONObject) connectionHashMap.get(connectionPath);
							 log.info("connectionFile="+connectionFile.toString());
							 String connectionUUID= (String) connectionFile.get("uuid");
							 dataStore.put("uuid", connectionUUID);
							 json.put("dataStore", dataStore);
							
							String result=datameerWatchDog.createImportJob(username,
									password, json.toString(),
									hostId, portNo);
							if (result.contains("success")) {
								JSONObject importJobJSON = (JSONObject) parser
										.parse(result);
								Long importJobId = (Long) importJobJSON.get("configuration-id");
								String importJobFullDetails = datameerWatchDog
										.readImportJob(username, password, hostId,
												portNo, importJobId + "");
								if (importJobFullDetails!=null)
								{
									JSONObject importJobFullDetailsJSON = (JSONObject) parser
											.parse(importJobFullDetails);
									 JSONObject file = (JSONObject)importJobFullDetailsJSON.get("file");
									 String path= (String)file.get("path");
									 importHashMap.put(path, file);
									
								}
								String importExecuteResult = datameerWatchDog.executeImportJob(username,
										password, hostId, portNo , importJobId + "");
								
								if (!importExecuteResult.contains("success")) {
									throw new RuntimeException(" Import job "+key+" not executed successfully ");
								}
							}
							else 
							{
								//throw new RuntimeException(" Import job "+key+" not created successfully ");
							}
							
						}

						if (key.contains(".wbk")) {

							JSONArray sheets = (JSONArray)json.get("sheets");
							int sheetsSize=sheets.size();
							for(int index=0;index<sheetsSize;index++) {
								JSONObject jsonSheet= (JSONObject)sheets.get(index);
								JSONObject dataSource= (JSONObject)(jsonSheet.get("datasource"));
								String importJobPath= (String)dataSource.get("path");
								JSONObject importJobDetails =(JSONObject) importHashMap.get(importJobPath);
								String importJobUUID= (String) importJobDetails.get("uuid");
								dataSource.put("uuid", importJobUUID);
								jsonSheet.put("datasource", dataSource);
								sheets.remove(index);
								sheets.add(index, jsonSheet);
								
							}
							json.put("sheets", sheets);
							
							String result=datameerWatchDog.createWorkBookJob(username,
									password, json.toString(),
									hostId, portNo);
							if (result.contains("success")) {
								JSONObject workbookJobJSON = (JSONObject) parser
										.parse(result);
								Long workbookJobId = (Long) workbookJobJSON.get("configuration-id");
								String workbookJobFullDetails = datameerWatchDog
										.readWorkBookJob(username, password, hostId,
												portNo, workbookJobId + "");
								if (workbookJobFullDetails!=null)
								{
									JSONObject workbookJobFullDetailsJSON = (JSONObject) parser
											.parse(workbookJobFullDetails);
									 JSONObject file = (JSONObject)workbookJobFullDetailsJSON.get("file");
									 String path= (String)file.get("path");
									 workbookHashMap.put(path, file);
									
								}
								String workbookExecuteResult = datameerWatchDog.executeWorkBookJob(username,
										password, hostId, portNo , workbookJobId + "");
								
								if (!workbookExecuteResult.contains("success")) {
									throw new RuntimeException(" workbook  "+key+" not executed successfully ");
								}
								
							}
							else 
							{
								//throw new RuntimeException(" workbook  "+key+" not created successfully ");
							}

						}

						if (key.contains(".exp")) {
							JSONObject sheet= (JSONObject)json.get("sheet");
							JSONObject workbook= (JSONObject)sheet.get("workbook");
							String workbookPath= (String)workbook.get("path");
							JSONObject workbookFile =(JSONObject) workbookHashMap.get(workbookPath);
							
							log.info("workbookFile="+workbookFile);
							String workbookUUID= (String) workbookFile.get("uuid");
							 
							
							workbook.put("uuid", workbookUUID);
							sheet.put("workbook", workbook);
							json.put("sheet", sheet);
							String result=datameerWatchDog.createExportJob(username,
									password, json.toString(),
									hostId, portNo);
							if (result.contains("success")) 
							{
																
							}
							else
							{
								throw new RuntimeException(" export job  "+key+" not created successfully ");
							}
						}

					//}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
