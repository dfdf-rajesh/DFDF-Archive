package com.dragonflyfactory.factory.db;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;

public class HashMapSerialized  implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	HashMap map;
	
	private HashMap getMap() {
		return map;
	}

	private void setMap(HashMap map) {
		this.map = map;
	}

	public HashMapSerialized(HashMap map) {
		this.map=map;
	}
	
	public byte[] getHashMapSerializedBytes() {
		
		try {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream os = new ObjectOutputStream(out);
	    os.writeObject(map);
	    return out.toByteArray();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		
		return null;
	}

}
