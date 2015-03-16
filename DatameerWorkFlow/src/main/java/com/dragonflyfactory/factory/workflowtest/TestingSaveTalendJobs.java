package com.dragonflyfactory.factory.workflowtest;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

public class TestingSaveTalendJobs {
	
	static Logger log = Logger.getLogger(TestingSaveTalendJobs.class.getName());

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try{
			
			ArrayList<String> givenFileNameList = new ArrayList<String>();
			givenFileNameList.add("C:/Users/DRAGONFLY DF/Desktop/Talend/Test_0.1.zip");
			getAllTalendJobsandTasks(givenFileNameList);
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}
	
	public static HashMap getAllTalendJobsandTasks(ArrayList<String> fileNameList) {
		
		HashMap<String,Object> map = new HashMap<String, Object>();
		
		try{
			
			if (fileNameList!=null && fileNameList.size()!=0)
			{
				for(String fileName:fileNameList) {
					
					FileReader fileReader=new FileReader(fileName);
					StringBuilder content=new StringBuilder("");
					int ch=0;
					while((ch=fileReader.read())!=-1) {
						content.append((char)ch);
					}
					fileReader.close();
					log.info(" the file "+fileName+" content  length="+content.length());
					map.put(fileName, content.toString());
					
				}
			}
			return map;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return null;
	}

}
