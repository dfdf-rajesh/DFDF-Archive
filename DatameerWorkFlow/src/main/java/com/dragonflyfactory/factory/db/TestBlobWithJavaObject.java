package com.dragonflyfactory.factory.db;
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
import java.util.HashMap;
import java.sql.Blob;

import javax.sql.rowset.serial.SerialBlob;

import com.dragonflyfactory.factory.workflowsave.AllJobsInOneHashMap;

public class TestBlobWithJavaObject 
{
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://twitternewyork.c3lq996wo33i.us-east-1.rds.amazonaws.com:3306/use_case_3";

   static final String USER = "dragonfly";
   static final String PASS = "datafactory";
   
   public static void main(String[] args) 
   {
	   //insertDatameerThingsIntoMysqlDb("1");
	   createDatameerThingsFromMySqlDB("6739");
   }
   
   public static int insertDatameerThingsIntoMysqlDb(String param) {

	   Connection conn = null;	   
	   Statement stmt = null;
	   
	   int size=0;
	   
	   try{

		   HashMap map = AllJobsInOneHashMap.giveDatameerObjectsASMap("Afzal", "admin", "54.86.180.167", "7777");
		   size = map.size();
		   if (size==0) {
			   return size;
		   }
		   
		   HashMapSerialized hashMapSerialized=new HashMapSerialized(map);

		   
		   Class.forName("com.mysql.jdbc.Driver");

		   System.out.println("Connecting to database...");
		   conn = DriverManager.getConnection(DB_URL,USER,PASS);
   
		   
           
           // To store the meta data of jobs of datameer as Binary Large Object into MYSQL
//		   String clearSql = "delete from TestBlob ";
//		   stmt = conn.createStatement();
//		   int status = stmt.executeUpdate(clearSql);
//		   System.out.println("status="+status);
           //String insertSql = "INSERT INTO TestBlob values (?)";
		   String insertSql = "INSERT INTO datameer_table values (?,?)";
           PreparedStatement statement = conn.prepareStatement(insertSql);
           
//           FileSerialize fs = new FileSerialize();
//           fs.writeObject2File(map);
//           
//           InputStream inputStream = new FileInputStream(new File("DatameerMetadata.dm"));
           
           Blob blob= new SerialBlob(hashMapSerialized.getHashMapSerializedBytes());
//         
           statement.setString(1, param);
           statement.setBlob(2,blob );
           
          // statement.setBlob(1, hashMapSerialized.);

           int row = statement.executeUpdate();
           if (row > 0) {
               //System.out.println("A contact was inserted with JSON file.");
        	   System.out.println("some jobs are stored into datameer_table.");
           }
           
           conn.close();
           
	   }catch(SQLException se){
		   se.printStackTrace();
		   return -1;
	   }catch(Exception e){
		   e.printStackTrace();
		   return -1;
	   }  
   
	   return size;
   }
   
   public static int createDatameerThingsFromMySqlDB(String param) {
	   

	   Connection conn = null;	   
	   Statement stmt = null;
	   
	   int size=-1;
	   
	   if (param==null || param.equals("")) {
		   return size;
	   }
	   
	   try{
		   Class.forName("com.mysql.jdbc.Driver");

		   System.out.println("Connecting to database...");
		   conn = DriverManager.getConnection(DB_URL,USER,PASS);
           
           // To retrieve the blob data from MYSQL
		   //String sql = "select * from TestBlob";
		   String sql = "select jobs_blob from datameer_table where id = '"+param+"'";
           stmt = conn.createStatement();
           HashMap newMap=null;
           ResultSet rs = stmt.executeQuery(sql);
           System.out.println(rs);
           
           if(rs.getRow() == 0 && rs.next())
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
           else
           {
        	   System.out.println("No record");
        	   return size;
           }
           conn.close();
           size=newMap.size();
    	   AllJobsInOneHashMap.createGivenDatameerObjectsASMapIntoDataMeerJobs(newMap, "Afzal", "admin", "54.86.180.167", "7777");
           
	   }catch(SQLException se){
		   se.printStackTrace();
	   }catch(Exception e){
		   e.printStackTrace();
	   }  
   return size;
   }
}