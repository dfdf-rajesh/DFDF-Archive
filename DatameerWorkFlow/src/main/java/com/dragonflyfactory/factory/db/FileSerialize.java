package com.dragonflyfactory.factory.db;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class FileSerialize 
{
	public boolean writeObject2File(Object map)
	{
		try
		{
			FileOutputStream fileOut =	new FileOutputStream("DatameerMetadata.dm");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(map);
			out.close();
			fileOut.close();
			System.out.printf("Serialized data is saved in DatameerMetadata.dm");
		}catch(IOException i)
		{
			i.printStackTrace();
		}
		return true;
	}
}
