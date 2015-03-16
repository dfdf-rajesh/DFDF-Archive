package com.dragonflyfactory.factory.db;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import javax.sql.rowset.serial.SerialBlob;

import com.dragonflyfactory.factory.workflowtest.TestingSaveTalendJobs;

public class TestTalendBlob 
{
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://twitternewyork.c3lq996wo33i.us-east-1.rds.amazonaws.com:3306/use_case_3";

   static final String USER = "dragonfly";
   static final String PASS = "datafactory";
   
   public static void main(String[] args) 
   {
	   Connection conn = null;	   
	   Statement stmt = null;
	   
	   try{
		   Class.forName("com.mysql.jdbc.Driver");

		   System.out.println("Connecting to database...");
		   conn = DriverManager.getConnection(DB_URL,USER,PASS);
		   
		   ArrayList<String> givenFileNameList = new ArrayList<String>();
		   givenFileNameList.add("C:/Users/DRAGONFLY DF/Desktop/Talend/Test_0.1/lib/userRoutines.jar");
		   givenFileNameList.add("C:/Users/DRAGONFLY DF/Desktop/Talend/Test_0.1/Test/test_0_1.jar");
		   
		   HashMap map = TestingSaveTalendJobs.getAllTalendJobsandTasks(givenFileNameList);
		   
		   HashMapSerialized hashMapSerialized=new HashMapSerialized(map);
           
           // To store the File data into Binary Large Object from MYSQL
		   
           String sql1 = "INSERT INTO TestBlobTalend values (?)";
           PreparedStatement statement = conn.prepareStatement(sql1);
           
//           FileSerialize fs = new FileSerialize();
//           fs.writeObject2File(map);
//           
//           InputStream inputStream = new FileInputStream(new File("DatameerMetadata.dm"));
           
           Blob blob= new SerialBlob(hashMapSerialized.getHashMapSerializedBytes());
//           
           statement.setBlob(1,blob );
           
          // statement.setBlob(1, hashMapSerialized.);

           int row = statement.executeUpdate();
           if (row > 0) {
               System.out.println("A contact was inserted with JSON file.");
           }
           
           // To retrieve the blob data from MYSQL
		   String sql = "select * from TestBlobTalend";		   
           stmt = conn.createStatement();
           HashMap newMap=null;
           ResultSet rs = stmt.executeQuery(sql);
           System.out.println(rs);
           if(rs.getRow() == 0)
           {
        	   while(rs.next())
        	   {
        		   Blob b = rs.getBlob(1);
        		   InputStream inputStream = b.getBinaryStream();

        		   try
        		   {	
        			   ObjectInputStream out = new ObjectInputStream(inputStream);
        			   newMap = (HashMap) out.readObject();
        			   out.close();
        			   inputStream.close();

        			   System.out.println(newMap);
        		   }catch(IOException i)
        		   {
        			   i.printStackTrace();        	       
        		   }
        	   }
        	   //AllJobsInOneHashMap.createGivenDatameerObjectsASMapIntoDataMeerJobs(newMap, "", "", "", "");
        	   for(Object key: newMap.keySet())
        	   {
        		   String fileName =  (String) key;
        		   System.out.println(fileName);
        		   System.out.println("New File:"+fileName.substring(0,fileName.length()-4)+"Copy"+fileName.substring(fileName.length()-4,fileName.length()));
        		   String content = (String) newMap.get(key);
        		   FileWriter fw = new FileWriter(fileName.substring(0,fileName.length()-4)+"Copy"+fileName.substring(fileName.length()-4,fileName.length()));
        		   fw.write(content);
        		   System.out.println(" size of file created = "+content.length());
        		   fw.close();
        	  }
           }
           else
           {
        	   System.out.println("No record");
           }
           conn.close();
           
	   }catch(SQLException se){
		   se.printStackTrace();
	   }catch(Exception e){
		   e.printStackTrace();
	   }  
   }
}