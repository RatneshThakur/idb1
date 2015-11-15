package sqlEngine;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.SchemaManager;

public class DeleteStatement extends Statement
{
	//Inherited member variables are in comments below
	//MainMemory mem;
	//Disk disk;   
	//SchemaManager schema_manager;
	//String stmt;
		
	Relation relation_reference;
	String relation_name;
	
	public DeleteStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		super(stmt_var,mem_var,disk_var,schema_manager_var);
	}
	
	public void runStatement()
	{
		String pattern1 = "FROM";
		String pattern2 = "WHERE";
		String whereCondition = "";
		ArrayList<String> tableNames = new ArrayList<String>();
		Pattern p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
		Matcher m = p.matcher(stmt);
		String allTables = "";
		while(m.find())
		{
			allTables = m.group(1);				
		}
		
		String[] allTablesSplit = allTables.split(",");
		for(int i=0; i<allTablesSplit.length; i++)
		{
			tableNames.add(allTablesSplit[i].trim());
		}
		
		//now getting the condition from where clause
		p = Pattern.compile(Pattern.quote(pattern2) + "(.*)");
		m = p.matcher(stmt);			
		if ( m.find() ) {
			whereCondition = m.group(1);
		}
		System.out.println(" Where condition is " + whereCondition);
		
		
	}
}

