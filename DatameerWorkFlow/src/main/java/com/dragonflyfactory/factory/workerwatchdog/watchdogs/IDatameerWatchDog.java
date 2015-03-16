package com.dragonflyfactory.factory.workerwatchdog.watchdogs;



public interface IDatameerWatchDog {
	

	//curl -u <username>:<password> -X POST -d @<job-payload>.json http://<Datameer-serverIP>:<port-number>/rest/connections
	public String createConnection(String username,String password,String jobPayLoad,String datameerServerIP,String portNumber);
	
	//curl -u <username>:<password> -X GET http://<Datameer-serverIP>:<port-number>/rest/connections/<job-configuration-id>
	public String readConnection(String username,String password,String datameerServerIP,String portNumber,String jobConfigurationId);
	
	//curl -u <username>:<password> -X PUT -d @<job-payload>.json http://<Datameer-serverIP>:<port-number>/rest/connections/<job-configuration-id>
	//public String updateConnection(String username,String password,String jobPayLoad,String datameerServerIP,String portNumber,String jobConfigurationId);
	
	//curl -u <username>:<password> -X DELETE http://<Datameer-serverIP>:<port-number>/rest/connections/<job-configuration-id>
	public String deleteConnection(String username,String password,String datameerServerIP,String portNumber,String jobConfigurationId);
	
	//curl -u <username>:<password> -X GET http://<Datameer-serverIP>:<port-number/rest/connections
	//public String listAllConnection(String username,String password,String datameerServerIP,String portNumber);
	
	//curl -u <username>:<password> -X POST -d @<job-payload>.json http://<Datameer-server-IP>:<port-number>/rest/import-job
	public String createImportJob(String username,String password,String jobPayLoad,String datameerServerIP,String portNumber);
	
	//curl -u <username>:<password> -X GET http://<Datameer-serverIP>:<port-number>/rest/import-job/<job-configuration-id>
	public String readImportJob(String username,String password,String datameerServerIP,String portNumber,String importJobConfigurationId);
	
	//curl -u <username>:<password> -X POST http://<Datameer-server-IP>:<port-number>/rest/job-execution?configuration=<import-job-configuration-id>
	public String executeImportJob(String username,String password,String datameerServerIP,String portNumber,String importJobConfigurationId);
	
	
	//curl -u <username>:<password> -X POST -d @<job-payload>.json http://<Datameer-serverIP>:<port-number>/rest/workbook
	public String createWorkBookJob(String username,String password,String jobPayLoad,String datameerServerIP,String portNumber);
	
	//curl -u <username>:<password> -X GET http://<Datameer-serverIP>:<port-number>/rest/workbook/<workbook-configuration-id>
	public String readWorkBookJob(String username,String password,String datameerServerIP,String portNumber,String workbookConfigurationId);
	
	//curl -u <username>:<password> -X POST http://<Datameer-serverIP>:<port-number>/rest/job-execution/?configuration=<workbook-configuration-id>
	public String executeWorkBookJob(String username,String password,String datameerServerIP,String portNumber,String workbookExecutionId);
	
	//curl -u <username>:<password> -X POST -d @<job-payload>.json http://<Datameer-serverIP>:<port-number>/rest/export-job
	public String createExportJob(String username,String password,String jobPayLoad,String datameerServerIP,String portNumber);
	
	//curl -u <username>:<password> -X GET http://<Datameer-serverIP>:<port-number>/rest/export-job/<export-job-configuration-id>
	public String readExportJob(String username,String password,String datameerServerIP,String portNumber,String exportJobkConfigurationId);
	
	//curl -u <username>:<password> -X POST http://<Datameer-serverIP>:<port-number>/rest/job-execution?configuration=<export-job-configuration-id>
	public String executeExportJob(String username,String password,String datameerServerIP,String portNumber,String exportJobConfigurationId);
	
}
