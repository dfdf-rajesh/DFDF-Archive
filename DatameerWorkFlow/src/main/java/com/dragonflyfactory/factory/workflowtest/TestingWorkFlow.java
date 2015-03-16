package com.dragonflyfactory.factory.workflowtest;

import java.util.ArrayList;

import com.dragonflyfactory.factory.workerwatchdog.watchdogs.DatameerWatchDog;
import com.dragonflyfactory.factory.workerwatchdog.watchdogs.IDatameerWatchDog;
import com.dragonflyfactory.factory.workflowctrl.JSONPayLoad;


import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class TestingWorkFlow {

	static Logger log = Logger.getLogger(TestingWorkFlow.class.getName());
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String workbookName=null;
		String workbookPath=null;
		String importJobName=null;
		String importJobPath=null;
		String importJobTable=null;
		String importJobOrderByColumn=null;
		String connectionName=null;
		String connectionPath=null;
		String connectionUserName=null;
		String connectionPassword=null;
		String connectionUrl=null;
		
		IDatameerWatchDog datameerWatchDog = new DatameerWatchDog();
		JSONParser parser = new JSONParser();
		try {
			String workbookDetailsJson = datameerWatchDog.readWorkBookJob("admin", "dragon", "54.86.180.167", "7777","549");
			Object obj = parser.parse(workbookDetailsJson);
			JSONObject jsonObj =(JSONObject) obj;
			
			JSONObject file =(JSONObject)jsonObj.get("file");
			String workbook_Path =(String)file.get("path");
			String workbook_uuid =(String)file.get("uuid");
			String workbook_name =(String)file.get("name");
			
			log.info("workbook_Path="+workbook_Path);
			log.info("workbook_uuid="+workbook_uuid);
			log.info("workbook_name="+workbook_name);
			
			if(workbook_name!=null && !workbook_name.equals("")) {
				workbookName=workbook_name;
				log.info("workbookName="+workbookName);
			}

			if(workbook_Path!=null && !workbook_Path.equals("")) {
				workbookPath=workbook_Path.substring(0,workbook_Path.lastIndexOf('/'));
				log.info("workbookPath="+workbookPath);
			}

			JSONArray sheets =(JSONArray)jsonObj.get("sheets");
			JSONObject sheet=(JSONObject)sheets.get(0);
			JSONObject datasource =(JSONObject)sheet.get("datasource");
			String datasourcePath =(String)datasource.get("path");
			String datasource_uuid =(String)datasource.get("uuid");
			
			log.info(" workbook datasourcePath="+datasourcePath);
			log.info(" workbook datasource_uuid="+datasource_uuid);

			if(datasourcePath!=null && !datasourcePath.equals("")) {
				importJobPath=datasourcePath.substring(0,datasourcePath.lastIndexOf('/'));
				log.info("importJobPath="+importJobPath);
			}
			String allImportJobDetails = datameerWatchDog.readImportJob("admin", "dragon", "54.86.180.167", "7777","");
			
			int ourImportJobDetailsStartingIndex=allImportJobDetails.indexOf(datasource_uuid);
			String ourImportJobDetails="";
			
			if (ourImportJobDetailsStartingIndex>12) {
				ourImportJobDetails=allImportJobDetails.substring(ourImportJobDetailsStartingIndex-12);
				int position1,position2,length;
				length=ourImportJobDetails.length();
				position1=ourImportJobDetails.indexOf('{');
				position2=ourImportJobDetails.indexOf('}');
				ourImportJobDetails=ourImportJobDetails.substring(position1);
				ourImportJobDetails=ourImportJobDetails.substring(0,position2+1);
				log.info("ourImportJobDetails="+ourImportJobDetails);
			}
			Long importJobId=(long)0;
			Long connectionId=(long)0;
			int ourConnectionDetailsStartingIndex=0;
			if (!ourImportJobDetails.equals("")) {
				JSONObject ourImportJob = (JSONObject)parser.parse(ourImportJobDetails);
				importJobId =  (Long)ourImportJob.get("id");
				log.info("importJobId="+importJobId);
			}
			String importJobFullDetails="";
			if (importJobId!=0) {
				
				importJobFullDetails=datameerWatchDog.readImportJob("admin", "dragon", "54.86.180.167", "7777",""+importJobId);
				JSONObject importJobFullDetailsJSON = (JSONObject)parser.parse(importJobFullDetails);
				JSONObject importJobdataStore =(JSONObject)importJobFullDetailsJSON.get("dataStore");
				JSONObject importJobFile =(JSONObject)importJobFullDetailsJSON.get("file");
				String importJob_Name=(String)importJobFile.get("name");
				log.info("importJob_Name="+importJob_Name);
				if (importJob_Name!=null && !importJob_Name.equals("")) {
					importJobName=importJob_Name;
					log.info("importJobName="+importJobName);
				}
				JSONObject properties =(JSONObject)importJobFullDetailsJSON.get("properties");
				ArrayList tableList=(ArrayList)properties.get("jdbc.property.table");
				ArrayList orderColumnList=(ArrayList)properties.get("orderColumn");
				log.info(" table = "+tableList.get(0));
				String table=(String)tableList.get(0);
				log.info(" orderColumn = "+orderColumnList.get(0));
				String orderColumn=(String)orderColumnList.get(0);
				if (table!=null && !table.equals("")) {
					importJobTable=table.substring(1,table.length()-1);
					log.info("importJobTable="+importJobTable);
				}
				if (orderColumn!=null && !orderColumn.equals("")) {
					importJobOrderByColumn=orderColumn;
					log.info("importJobOrderByColumn="+importJobOrderByColumn);
				}
				String importJobdataStorePath =(String)importJobdataStore.get("path");
				String importJobdataStore_uuid =(String)importJobdataStore.get("uuid");
				log.info("importJobdataStorePath="+importJobdataStorePath);
				log.info("importJobdataStore_uuid="+importJobdataStore_uuid);
				if (importJobdataStorePath!=null && !importJobdataStorePath.equals("")) {
					connectionPath=importJobdataStorePath.substring(0,importJobdataStorePath.lastIndexOf('/'));
					log.info("connectionPath="+connectionPath);
				}
				String allConnectionDetails=datameerWatchDog.readConnection("admin", "dragon", "54.86.180.167", "7777","");
				ourConnectionDetailsStartingIndex=allConnectionDetails.indexOf(importJobdataStore_uuid);
				String ourConnectionDetails="";
				if (ourConnectionDetailsStartingIndex>12) {
					
					ourConnectionDetails=allConnectionDetails.substring(ourConnectionDetailsStartingIndex-12);
					int position1,position2;
					//int length;
					//length=ourConnectionDetails.length();
					position1=ourConnectionDetails.indexOf('{');
					position2=ourConnectionDetails.indexOf('}');
					ourConnectionDetails=ourConnectionDetails.substring(position1);
					ourConnectionDetails=ourConnectionDetails.substring(0,position2+1);
					log.info("ourConnectionDetails="+ourConnectionDetails);
					JSONObject ourConnectionDetailsJSON = (JSONObject)parser.parse(ourConnectionDetails);
					connectionId=(Long)ourConnectionDetailsJSON.get("id");
					log.info("our connection id="+connectionId);

				}
				String connectionFullDetails="";
				if (connectionId!=0) {
					
					connectionFullDetails=datameerWatchDog.readConnection("admin", "dragon", "54.86.180.167", "7777",""+connectionId);
					log.info("our connectionFullDetails ="+connectionFullDetails);
					JSONObject connectionFullDetailsJSON = (JSONObject)parser.parse(connectionFullDetails);
					
					JSONObject connectionFile =(JSONObject)connectionFullDetailsJSON.get("file");
					String connection_Path =(String)connectionFile.get("path");
					String connection_Name =(String)connectionFile.get("name");
					log.info("connection_Name="+connection_Name);
					if (connection_Name!=null && !connection_Name.equals("")) {
						connectionName=connection_Name;
						log.info("connectionName="+connectionName);
					}
					log.info("our connectionPath="+connection_Path);
					JSONObject connectionProperties =(JSONObject)connectionFullDetailsJSON.get("properties");
					ArrayList userNameList=(ArrayList)connectionProperties.get("user_name");
					ArrayList passwordList=(ArrayList)connectionProperties.get("password");
					ArrayList connectionUrlList=(ArrayList)connectionProperties.get("key.connectionUrl");
					log.info(" userName = "+userNameList.get(0));
					String userName=(String)userNameList.get(0);
					log.info(" password = "+passwordList.get(0));
					String password=(String)passwordList.get(0);
					log.info(" connection_Url = "+connectionUrlList.get(0));
					String connection_Url=(String)connectionUrlList.get(0);
					
					if(userName!=null && !userName.equals("")) {
						connectionUserName=userName;
						log.info("connectionUserName="+connectionUserName);
					}
					
					if(password!=null && !password.equals("")) {
						connectionPassword=password;
						log.info("connectionPassword="+connectionPassword);
					}

					if(connection_Url!=null && !connection_Url.equals("")) {
						connectionUrl=connection_Url;
						log.info("connectionUrl="+connectionUrl);
					}

				}

			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		log.info("workbookName="+workbookName);
		log.info("workbookPath="+workbookPath);
		log.info("importJobName="+importJobName);
		log.info("importJobPath="+importJobPath);
		log.info("importJobTable="+importJobTable);
		log.info("importJobOrderByColumn="+importJobOrderByColumn);
		log.info("connectionName="+connectionName);
		log.info("connectionPath="+connectionPath);
		log.info("connectionUserName="+connectionUserName);
		log.info("connectionPassword="+connectionPassword);
		log.info("connectionUrl="+connectionUrl);
		
		String newConnectionName=connectionName+"_Copy";
		String newImportJobName=importJobName+"_Copy";
		String newWorkbookName=workbookName+"_Copy";
		
		Long newConnectionId=(long)0;
		Long newImportJobId=(long)0;
		Long newWorkbookId=(long)0;
		
		String newConnection_uiid=null;
		String newConnection_path=null;
		
		String newImportJob_uiid=null;
		String newImportJob_path=null;

		String newWorkbook_uiid=null;
		String newWorkbook_path=null;

		String successMessage="success";
		String status="status";
		
		try {
			String fileName=JSONPayLoad.createMySqlConnectionPayLoad(newConnectionName,connectionPath, connectionUrl,
											"IMPORT_EXPORT", connectionUserName, connectionPassword, "Mysql");
			String newConnectionResult=datameerWatchDog.createConnection("admin", "dragon", fileName,"54.86.180.167", "7777");
			
			JSONObject newConnectionResultJSON=(JSONObject)parser.parse(newConnectionResult);
			
			String newConnectionStatus=(String)newConnectionResultJSON.get(status);
			
			if (newConnectionStatus.equals(successMessage))
			{
				newConnectionId=(Long)newConnectionResultJSON.get("configuration-id");
			}
			
			if (newConnectionId!=0) {
				
				String newConnectionDetails=datameerWatchDog.readConnection("admin", "dragon", "54.86.180.167", "7777",""+newConnectionId);
				
				JSONObject newConnectionDetailsJSON=(JSONObject)parser.parse(newConnectionDetails);
				
				JSONObject newConnectionFile =(JSONObject)newConnectionDetailsJSON.get("file");
				String newConnectionPath =(String)newConnectionFile.get("path");
				String newConnectionUUID =(String)newConnectionFile.get("uuid");
				
				if (newConnectionPath!=null && !newConnectionPath.equals("")) {
					newConnection_path=newConnectionPath;
				}
				
				if (newConnectionUUID!=null && !newConnectionUUID.equals("")) {
					newConnection_uiid=newConnectionUUID;
				}
				
				String importPayLoadFileName=JSONPayLoad.createMySqlImportJobPayLoad(newImportJobName, importJobPath, "", connectionUrl,
										connectionUserName, "datafactory", importJobTable, newConnection_path, newConnection_uiid);
				
				String newImportJobResult=datameerWatchDog.createImportJob("admin", "dragon", importPayLoadFileName,"54.86.180.167", "7777");
				
				JSONObject newImportJobResultJSON=(JSONObject)parser.parse(newImportJobResult);
				
				String newImportJobStatus=(String)newImportJobResultJSON.get(status);
				
				if (newImportJobStatus.equals(successMessage))
				{
					newImportJobId=(Long)newImportJobResultJSON.get("configuration-id");
				}
				
				if (newImportJobId!=0) {
					
					String newImportJobDetails=datameerWatchDog.readImportJob("admin", "dragon", "54.86.180.167", "7777",""+newImportJobId);
					
					JSONObject newImportJobDetailsJSON=(JSONObject)parser.parse(newImportJobDetails);
					
					JSONObject newImportJobFile =(JSONObject)newImportJobDetailsJSON.get("file");
					String newImportJobPath =(String)newImportJobFile.get("path");
					String newImportJobUUID =(String)newImportJobFile.get("uuid");
					
					if (newImportJobPath!=null && !newImportJobPath.equals("")) {
						newImportJob_path=newImportJobPath;
					}
					
					if (newImportJobUUID!=null && !newImportJobUUID.equals("")) {
						newImportJob_uiid=newImportJobUUID;
					}
					
					String newImportJobExecuteDetails=datameerWatchDog.executeImportJob("admin", "dragon", "54.86.180.167", "7777",""+newImportJobId);
					
					if (newImportJobExecuteDetails.contains(successMessage)) {
						Thread.sleep(60000);
					}
					else {
						throw new RuntimeException(" Import Job not executed succussfully");
					}
					
					String workbookPayLoadFileName = JSONPayLoad.createWorkBookJobPayLoad(newWorkbookName, workbookPath, newImportJob_path, newImportJob_uiid, newImportJobName);
					
					String newWorkbookResult=datameerWatchDog.createWorkBookJob("admin", "dragon", workbookPayLoadFileName,"54.86.180.167", "7777");
					
					JSONObject newWorkbookResultJSON=(JSONObject)parser.parse(newWorkbookResult);
					
					String newWorkbookStatus=(String)newWorkbookResultJSON.get(status);
					
					if (newWorkbookStatus.equals(successMessage))
					{
						newWorkbookId=(Long)newWorkbookResultJSON.get("configuration-id");
					}
					
					if (newWorkbookId!=0) {
						
						String newWorkbookExecuteDetails=datameerWatchDog.executeWorkBookJob("admin", "dragon", "54.86.180.167", "7777",""+newWorkbookId);
						
						if (newWorkbookExecuteDetails.contains(successMessage)) {
							Thread.sleep(3000);
						}
						else {
							throw new RuntimeException(" Workbook Job not executed succussfully");
						}
					}
					
				}

			}
			
		}
		catch(Exception e)
		{
			log.info(" problem = "+e.getMessage());
			e.printStackTrace();
		}


	}

}
