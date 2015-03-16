package com.dragonflyfactory.factory.workflowctrl;

import org.apache.log4j.Logger;

//import com.dragonflyfactory.factory.Constants;
//import com.dragonflyfactory.factory.message.Request;
import com.dragonflyfactory.factory.workerwatchdog.watchdogs.DatameerWatchDog;
import com.dragonflyfactory.factory.workerwatchdog.watchdogs.IDatameerWatchDog;


public class DatameerWatchDogTest{
	
	static Logger log = Logger.getLogger(DatameerWatchDogTest.class.getName());
	

	public DatameerWatchDogTest() {
		
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		log.info("Welcome to create DatameerWatchDog Test!!!");
		
		

		//IDatameerWatchDog datameerWatchDog = new DatameerWatchDog(Constants.DATAMEER_WATCHDOG_QUEUE);
		IDatameerWatchDog datameerWatchDog = new DatameerWatchDog();
		try {
//			datameerWatchDog.readConnection("admin", "dragon", "54.86.180.167", "7777","67");
//			datameerWatchDog.readImportJob("admin", "dragon", "54.86.180.167", "7777","447");
//			datameerWatchDog.readWorkBookJob("admin", "dragon", "54.86.180.167", "7777","453");
//			datameerWatchDog.readExportJob("admin", "dragon", "54.86.180.167", "7777","");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		try {
//			String fileName=JSONPayLoad.createMySqlConnectionPayLoad("mysqlTest","", "jdbc:mysql://twitternewyork.c3lq996wo33i.us-east-1.rds.amazonaws.com:3306/twitter_tal", "IMPORT_EXPORT", "dragonfly", "SECURE:0:iFjB0XZ3uNcFapjPwmI1KrU1SpPylOdx", "Mysql");
//			String fileName=JSONPayLoad.createHDFSConnectionPayLoad("HDFSTest","IMPORT_EXPORT", "54.172.235.189","8020", "/");
//			datameerWatchDog.createConnection("admin", "admin",fileName , "54.86.180.167", "7777");
//			datameerWatchDog.createConnection("admin", "dragon","C:/Test/tryingMySqlWithoutCurl.json" , "54.86.180.167", "7777");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		try {
			//twitter_data
//			String fileName=JSONPayLoad.createMySqlImportJobPayLoad("JobWOCurl", "","twitter", "jdbc:mysql://twitternewyork.c3lq996wo33i.us-east-1.rds.amazonaws.com:3306/twitter", "dragonfly", "datafactory", "twitter_data_copy", "/Data/Connections/MySQL_Connection_notUsingCurl.dst", "6f13a105-981a-4cce-b555-7454f4708c62");
//			datameerWatchDog.createImportJob("admin", "dragon", fileName, "54.86.180.167", "7777");
//			datameerWatchDog.executeImportJob("admin", "dragon", "54.86.180.167", "7777","447");			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		try {
//			String fileName=JSONPayLoad.createWorkBookJobPayLoad("JobWOCurl", "/Data/ImportJobs/JobWOCurl.imp", "c717703b-9f58-477f-9f0c-720dce2fdb97","JobWOCurl");
//			datameerWatchDog.createWorkBookJob("admin", "dragon", fileName, "54.86.180.167", "7777");
//			datameerWatchDog.executeWorkBookJob("admin", "dragon", "54.86.180.167", "7777","453");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		try {
//			String fileName=JSONPayLoad.createExportJobPayLoad("JobWOCurl","/Data/Connections/tryingMySqlWithoutCurl.dst", "/Analytics/Workbooks/JobWOCurl.wbk", "88f1982f-b616-4b01-98e2-2ea247fee9c0","JobWOCurl");
//			datameerWatchDog.createExportJob("admin", "dragon", "C:/Test/JobWOCurlExportPayLoad.json", "54.86.180.167", "7777");
//			datameerWatchDog.executeExportJob("admin", "dragon", "54.86.180.167", "7777","463");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		


		}

	}
	
		


