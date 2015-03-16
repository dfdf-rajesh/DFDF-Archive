package com.dragonflyfactory.factory.workerwatchdog.watchdogs;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

//import net.sf.json.JSONObject;
//import net.sf.json.JSONSerializer;


import org.apache.log4j.Logger;


public class DatameerWatchDog  implements IDatameerWatchDog {

	static Logger log = Logger.getLogger(DatameerWatchDog.class.getName());


	private static String url_string=null;
	
	
	public DatameerWatchDog() {
		
		// TODO Auto-generated constructor stub
	}

	
	public static String getUrl_string() {
		return url_string;
	}

	public static void setUrl_string(String url_string) {
		DatameerWatchDog.url_string = url_string;
	}

	// Name of the queue Datameer watchdog we will receive messages from
	//private static String inQueue = Constants.DATAMEER_WATCHDOG_QUEUE;

	private static DatameerWatchDog datameerWWD = null;

	public static void main(String[] args) {

		log.info("Welcome to Datameer Worker WatchDog !!!");

		datameerWWD = new DatameerWatchDog();

		//datameerWWD.listenToMsg();
	}

//	private void listenToMsg() {
//
//		log.info("In listenToMsg of Datameer WorkerWatchDog");
//		// Keep listening - Need to see how to stop this.
//		while (true) {
//
//			try {
//				String qMessage = msgQueue.take();
//				log.info("Pulled message:" + qMessage);
//
//				JSONObject eventJson = (JSONObject) JSONSerializer
//						.toJSON(qMessage);
//				String action = (String) eventJson.get("workeraction");
//				// Depending on the action call the method
//				// For Datameer currently runjob is the action
//				if (action != null) {
//
//					processMessage(eventJson);
//				}
//
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//	}
//
//	public void processMessage(JSONObject eventJson) {
//		log.info("Entered processMessage Datameer WatchDog");
//		log.info("eventJson: " + eventJson.toString());
//
//		String responseUUID = (String) eventJson.get("uuid");
//		String responseName = (String) eventJson.get("name");
//
//		Response responeSubmission = new Response("Response for " + responseName);
//		responeSubmission.add("name", responseName);
//		responeSubmission.add("requestUUID", responseUUID);
//		responeSubmission.add("status", "Job Submitted");
//		responeSubmission.add("producer", "Datameer Worker WatchDog");
//		responeSubmission.add("tableType", "worker");
//		datameerWWD.addOutstandingRequest(responeSubmission);
//		responeSubmission.sendToQueue(Constants.WORKER_CONTROLLER_QUEUE);
//
//		String workeraction = (String) eventJson.get("workeraction");
//
//		try {
//
//			String response = null;
//			String username = (String) eventJson.get("username");
//			String password = (String) eventJson.get("password");
//			String serverIP = (String) eventJson.get("serverIP");
//			String portNo = (String) eventJson.get("portNo");
//			// checking "create" or "execute"
//			if (null != workeraction) {
//				if (workeraction.length() >= 6) {
//					if (workeraction.substring(0, 6).equalsIgnoreCase("create")) {
//						String payLoad = (String) eventJson.get("payLoad");
//
//						if (username != null && password != null
//								&& serverIP != null && portNo != null
//								&& payLoad != null)
//
//							if (workeraction
//									.equalsIgnoreCase("createConnection")) {
//								response = this.createConnection(username,
//										password, payLoad, serverIP, portNo);
//							} else if (workeraction
//									.equalsIgnoreCase("createImportJob")) {
//								response = this.createImportJob(username,
//										password, payLoad, serverIP, portNo);
//							} else if (workeraction
//									.equalsIgnoreCase("createWorkBookJob")) {
//								response = this.createWorkBookJob(username,
//										password, payLoad, serverIP, portNo);
//							} else if (workeraction
//									.equalsIgnoreCase("createExportJob")) {
//								response = this.createExportJob(username,
//										password, payLoad, serverIP, portNo);
//							}
//					} else if (workeraction.length() >= 7
//							&& workeraction.substring(0, 7).equalsIgnoreCase(
//									"execute")) {
//						if (username != null && password != null
//								&& serverIP != null && portNo != null)
//
//							if (workeraction
//									.equalsIgnoreCase("executeImportJob")) {
//								String importJobConfigurationId = (String) eventJson
//										.get("importJobConfigurationId");
//								if (importJobConfigurationId != null) {
//									response = this.executeImportJob(username,
//											password, serverIP, portNo,
//											importJobConfigurationId);
//								}
//							} else if (workeraction
//									.equalsIgnoreCase("executeWorkBookJob")) {
//								String workbookExecutionId = (String) eventJson
//										.get("workbookExecutionId");
//								if (workbookExecutionId != null) {
//									response = this.executeWorkBookJob(
//											username, password, serverIP,
//											portNo, workbookExecutionId);
//								}
//							} else if (workeraction
//									.equalsIgnoreCase("executeExportJob")) {
//								String exportJobConfigurationId = (String) eventJson
//										.get("exportJobConfigurationId");
//								if (exportJobConfigurationId != null) {
//									response = this.executeExportJob(username,
//											password, serverIP, portNo,
//											exportJobConfigurationId);
//								}
//							} else if (workeraction
//									.equalsIgnoreCase("executecommonworkerjob")) { // Papaiah
//								log.info("Entered into executecommonworkerjob");
//								String importJobConfigurationId = (String) eventJson
//										.get("importJobConfigurationId");
//								if (importJobConfigurationId != null) {
//									response = this.executeImportJob(username,
//											password, serverIP, portNo,
//											importJobConfigurationId);
//									log.info("importJobConfigurationId rsponse: "
//											+ response);
//								}
//
//								String workbookExecutionId = (String) eventJson
//										.get("workbookExecutionId");
//								if (workbookExecutionId != null) {
//									response = this.executeWorkBookJob(
//											username, password, serverIP,
//											portNo, workbookExecutionId);
//									log.info("workbookExecutionId response: "
//											+ response);
//								}
//
//								String exportJobConfigurationId = (String) eventJson
//										.get("exportJobConfigurationId");
//								if (exportJobConfigurationId != null) {
//									response = this.executeExportJob(username,
//											password, serverIP, portNo,
//											exportJobConfigurationId);
//									log.info("exportJobConfigurationId response: "
//											+ response);
//								}
//							}
//					}
//				}
//
//				// Inform the Worker Controller that the Job has finished
//				if (response != null) {
//
////					log.info("Entered into response condition");
////					String requestUUID = (String) eventJson.get("uuid");
////					String name = (String) eventJson.get("name");
////
////					Response resp = new Response("Response for " + name);
////					resp.add("name", name);
////					resp.add("requestUUID", requestUUID);
////					resp.add("status", "Datameer Processing Completed");
////					resp.add("producer", "Datameer Worker WatchDog");
////					resp.add("tableType", "worker");
////					datameerWWD.addOutstandingRequest(resp);
////					resp.sendToQueue(Constants.WORKER_CONTROLLER_QUEUE);
////					log.info("Response sent successfully");
//
//				} else {
//					throw new RuntimeException();
//				}
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//			log.info("Datameer Job  Interrupted");
//		}
//		log.info("Datameer Job Complete");
//	}

	public String createConnection(String username, String password,
			String jobPayLoad, String datameerServerIP, String portNumber) {
		// TODO Auto-generated method stub
		// curl -u <username>:<password> -X POST -d @<job-payload>.json
		// http://<Datameer-serverIP>:<port-number>/rest/connections

		log.info("entering createConnection in DatameerWatchDog and jobPayLoad="
				+ jobPayLoad);
		try {

			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/connections";
			
			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/connections";
			}
			
			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			DataOutputStream sendingStream = new DataOutputStream(con.getOutputStream());
//			FileReader fileReader=new FileReader(jobPayLoad);
//			StringBuilder content=new StringBuilder("");
//			int ch=0;
//			while((ch=fileReader.read())!=-1) {
//				content.append((char)ch);
//			}
//			fileReader.close();
//			sendingStream.writeBytes(content.toString());//jobPayLoad
			sendingStream.writeBytes(jobPayLoad);//jobPayLoad
			sendingStream.flush();
			sendingStream.close();
			int responseCode=con.getResponseCode();

			Thread.sleep(1000);
			log.info("responseCode="+responseCode);
			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			log.info("leaving createConnection in DatameerWatchDog");
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String readConnection(String username, String password,
			String datameerServerIP, String portNumber,
			String jobConfigurationId) {
		// TODO Auto-generated method stub
		// curl -u <username>:<password> -X GET
		// http://<Datameer-serverIP>:<port-number>/rest/connections/<job-configuration-id>

		log.info("entering readConnection in DatameerWatchDog and id = "+jobConfigurationId);
		try {
			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/connections";
			
			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/connections";
			}


			if (jobConfigurationId!=null && !jobConfigurationId.equals("")) {

				url=url+"/"+jobConfigurationId;
			}

			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("GET");
			int responseCode=con.getResponseCode();
			Thread.sleep(1000);
			log.info("responseCode="+responseCode);
			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			log.info("leaving readConnection in DatameerWatchDog");
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String deleteConnection(String username, String password,
			String datameerServerIP, String portNumber,
			String jobConfigurationId) {
		// TODO Auto-generated method stub
		log.info("entering deleteConnection in DatameerWatchDog and id = "+jobConfigurationId);
		try {
			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/connections";
			
			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/connections";
			}


			if (jobConfigurationId!=null && !jobConfigurationId.equals("")) {

				url=url+"/"+jobConfigurationId;
			}

			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("DELETE");
			int responseCode=con.getResponseCode();
			Thread.sleep(1000);
			log.info("responseCode="+responseCode);
			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			log.info("leaving deleteConnection in DatameerWatchDog");
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	
	public String createImportJob(String username, String password,
			String jobPayLoad, String datameerServerIP, String portNumber) {
		// TODO Auto-generated method stub
		// curl -u <username>:<password> -X POST -d @<job-payload>.json
		// http://<Datameer-server-IP>:<port-number>/rest/import-job

		log.info("entering createImportJob in DatameerWatchDog");
		try {
			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/import-job";
			
			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/import-job";
			}

			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			DataOutputStream sendingStream = new DataOutputStream(con.getOutputStream());
			FileReader fileReader=new FileReader(jobPayLoad);
			StringBuilder content=new StringBuilder("");
			int ch=0;
			while((ch=fileReader.read())!=-1) {
				content.append((char)ch);
			}
			fileReader.close();
			sendingStream.writeBytes(content.toString());
			sendingStream.flush();
			sendingStream.close();
			int responseCode=con.getResponseCode();

			Thread.sleep(1000);
			log.info("responseCode="+responseCode);

			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			log.info("leaving createImportJob in DatameerWatchDog");
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String readImportJob(String username, String password,
			String datameerServerIP, String portNumber,
			String importJobConfigurationId) {
		// TODO Auto-generated method stub
		// curl -u <username>:<password> -X GET
		// http://<Datameer-serverIP>:<port-number>/rest/import-job/<job-configuration-id>

		log.info("entering readImportJob in DatameerWatchDog and id = "+importJobConfigurationId);
		try {
			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/import-job";

			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/import-job";
			}

			if (importJobConfigurationId!=null && !importJobConfigurationId.equals("")) {

				url=url+"/"+importJobConfigurationId;
			}

			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("GET");
			int responseCode=con.getResponseCode();
			Thread.sleep(1000);
			log.info("responseCode="+responseCode);
			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			log.info("leaving readImportJob in DatameerWatchDog");
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String executeImportJob(String username, String password,
			String datameerServerIP, String portNumber,
			String importJobConfigurationId) {
		// TODO Auto-generated method stub
		// curl -u <username>:<password> -X POST
		// http://<Datameer-server-IP>:<port-number>/rest/job-execution?configuration=<import-job-configuration-id>

		log.info("entering executeImportJob in DatameerWatchDog");
		try {
			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/job-execution?configuration="+importJobConfigurationId;
			
			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/job-execution?configuration="+importJobConfigurationId;
			}

			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("POST");
			int responseCode=con.getResponseCode();

			Thread.sleep(1000);
			log.info("responseCode="+responseCode);

			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			log.info("leaving executeImportJob in DatameerWatchDog");

			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String createWorkBookJob(String username, String password,
			String jobPayLoad, String datameerServerIP, String portNumber) {
		// TODO Auto-generated method stub
		// curl -u <username>:<password> -X POST -d @<job-payload>.json
		// http://<Datameer-serverIP>:<port-number>/rest/workbook

		log.info("entering createWorkBookJob in DatameerWatchDog and jobPayLoad="
				+ jobPayLoad);
		try {
			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/workbook";
			
			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/workbook";
			}

			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			DataOutputStream sendingStream = new DataOutputStream(con.getOutputStream());
			FileReader fileReader=new FileReader(jobPayLoad);
			StringBuilder content=new StringBuilder("");
			int ch=0;
			while((ch=fileReader.read())!=-1) {
				content.append((char)ch);
			}
			fileReader.close();
			sendingStream.writeBytes(content.toString());
			sendingStream.flush();
			sendingStream.close();
			int responseCode=con.getResponseCode();

			Thread.sleep(1000);
			log.info("responseCode="+responseCode);

			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String readWorkBookJob(String username, String password,
			String datameerServerIP, String portNumber,
			String workbookConfigurationId) {
		// TODO Auto-generated method stub
		// curl -u <username>:<password> -X GET
		// http://<Datameer-serverIP>:<port-number>/rest/workbook/<workbook-configuration-id>

		log.info("entering readWorkBookJob in DatameerWatchDog and id = "+workbookConfigurationId);
		try {
			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/workbook";

			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/workbook";
			}

			if (workbookConfigurationId!=null && !workbookConfigurationId.equals("")) {

				url=url+"/"+workbookConfigurationId;
			}

			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("GET");
			int responseCode=con.getResponseCode();
			Thread.sleep(1000);
			log.info("responseCode="+responseCode);
			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			log.info("leaving readWorkBookJob in DatameerWatchDog");
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String executeWorkBookJob(String username, String password,
			String datameerServerIP, String portNumber,
			String workbookExecutionId) {
		// TODO Auto-generated method stub
		// curl -u <username>:<password> -X POST
		// http://<Datameer-serverIP>:<port-number>/rest/job-execution/?configuration=<workbook-configuration-id>

		log.info("entering executeWorkBookJob in DatameerWatchDog");
		try {
			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/job-execution?configuration="+workbookExecutionId;
			
			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/job-execution?configuration="+workbookExecutionId;
			}

			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("POST");
			int responseCode=con.getResponseCode();

			Thread.sleep(1000);
			log.info("responseCode="+responseCode);

			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			log.info("leaving executeWorkBookJob in DatameerWatchDog");

			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String createExportJob(String username, String password,
			String jobPayLoad, String datameerServerIP, String portNumber) {
		// TODO Auto-generated method stub
		// curl -u <username>:<password> -X POST -d @<job-payload>.json
		// http://<Datameer-serverIP>:<port-number>/rest/export-job

		log.info("entering createExportJob in DatameerWatchDog");
		try {
			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/export-job";
			
			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/export-job";
			}

			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			DataOutputStream sendingStream = new DataOutputStream(con.getOutputStream());
			FileReader fileReader=new FileReader(jobPayLoad);
			StringBuilder content=new StringBuilder("");
			int ch=0;
			while((ch=fileReader.read())!=-1) {
				content.append((char)ch);
			}
			fileReader.close();
			sendingStream.writeBytes(content.toString());
			sendingStream.flush();
			sendingStream.close();
			int responseCode=con.getResponseCode();

			Thread.sleep(1000);
			log.info("responseCode="+responseCode);

			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String readExportJob(String username, String password,
			String datameerServerIP, String portNumber,
			String exportJobkConfigurationId) {
		// TODO Auto-generated method stub
		// curl -u <username>:<password> -X GET
		// http://<Datameer-serverIP>:<port-number>/rest/export-job/<export-job-configuration-id>

		log.info("entering readExportJob in DatameerWatchDog  and id = "+exportJobkConfigurationId);
		try {
			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/export-job";

			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/export-job";
			}

			if (exportJobkConfigurationId!=null && !exportJobkConfigurationId.equals("")) {

				url=url+"/"+exportJobkConfigurationId;
			}

			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("GET");
			int responseCode=con.getResponseCode();
			Thread.sleep(1000);
			log.info("responseCode="+responseCode);
			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			log.info("leaving readExportJob in DatameerWatchDog");
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String executeExportJob(String username, String password,
			String datameerServerIP, String portNumber,
			String exportJobConfigurationId) {
		// TODO Auto-generated method stub
		// curl -u <username>:<password> -X POST
		// http://<Datameer-serverIP>:<port-number>/rest/job-execution?configuration=<export-job-configuration-id>

		log.info("entering executeExportJob in DatameerWatchDog");
		try {
			String url ="http://"+datameerServerIP+":"+portNumber+"/rest/job-execution?configuration="+exportJobConfigurationId;
			
			if (getUrl_string()!=null) {
				
				url=getUrl_string()+"/rest/job-execution?configuration="+exportJobConfigurationId;
			}

			URL obj = new URL(url); 
			HttpURLConnection con =(HttpURLConnection) obj.openConnection();
			String userpass = username+":"+password;
			@SuppressWarnings("restriction")
			String basicAuth = "Basic " + new String( new sun.misc.BASE64Encoder().encode(userpass.getBytes()));
			con.setRequestProperty ("Authorization", basicAuth);
			con.setRequestMethod("POST");
			int responseCode=con.getResponseCode();

			Thread.sleep(1000);
			log.info("responseCode="+responseCode);

			String result = "";
			String line = "";
			if (responseCode==200) {
				BufferedReader read = new BufferedReader(new InputStreamReader(
						con.getInputStream()));
				while ((line = read.readLine()) != null) {
					result = result + line;
				}
				log.info("result=" + result);
			}
			else {
				log.info(" The request was not processed properly by datameer. Please check the input " );
				result=null;
			}
			log.info("leaving executeExportJob in DatameerWatchDog");

			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}




}
