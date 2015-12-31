package main.java.database;

import java.util.Map;

public abstract class AbstractDatabase {
	protected final String name;

	protected AbstractDatabase(String name){
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	protected abstract Boolean connectToDB(); 
	
	protected abstract void closeConnection();
	
	protected abstract void createTable();
	
	protected abstract void deleteTable();
	
	protected abstract void loadData(String filePath);  
	
	protected abstract void getData();
	
	public abstract Map<String,Object> runTest(String fileName);      
}
