package sqlEngine;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storageManager.Block;
import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.SchemaManager;
import storageManager.Tuple;

public class InsertStatement extends Statement
{
	Relation relation_reference;
	String relation_name;
	public InsertStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var, PrintWriter writer_var)
	{
		super(stmt_var,mem_var,disk_var,schema_manager_var,writer_var);
	}
	
	//Insert Statement
	public boolean runStatement()
	{
		
		String[] attrNames = null; // this will hold attribute names sid, homework etc
		String[] attrValues = null; // this will hold attribute values;
		relation_name = stmt.split(" ")[2];
		relation_reference = schema_manager.getRelation(relation_name);
		if(relation_reference == null)
		{
			System.out.println(relation_name + " does not exist.");
			return true;
		}
		// INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, "A")
		
		//parsing logic starts
		stmt = stmt.replace('(', '{');
		stmt = stmt.replace(')', '}');
		
		boolean isSelectPresent = false;
		
		List<String> allMatches = new ArrayList<String>();
		 Matcher m = Pattern.compile("\\{([^}]*)\\}")
		     .matcher(stmt);
		 while (m.find()) {
		   String matchedPart = m.group();
		   matchedPart= matchedPart.substring(1, matchedPart.length()-1);
		   allMatches.add(matchedPart);		   
		 }	
		
		 ArrayList<Tuple> valuesList = new ArrayList<Tuple>(); 
		 
		if(stmt.contains("SELECT"))
		{
			stmt = stmt.replace('{', '(');
			stmt = stmt.replace('}', ')');
			isSelectPresent = true;
			attrNames = allMatches.get(0).split(",");
			m = Pattern.compile("SELECT(.*)")
				     .matcher(stmt);
				 while (m.find()) {
				   String matchedPart = m.group();
				   //System.out.println(" select statement is " + matchedPart);				   
				   
				   SelectStatement selectStmt = new SelectStatement(stmt,mem,disk,schema_manager,writer);
				   valuesList = selectStmt.runStatement(true); //isPartOfQuery = true
 				 }
		}
		else
		{
			attrNames = allMatches.get(0).split(",");
			attrValues = allMatches.get(1).split(",");
			//removing extra spaces if any
			for(int i=0; i<attrNames.length; i++)
			{				
				attrValues[i] = attrValues[i].trim();
				attrNames[i] = attrNames[i].trim();
			}		
			
			//ArrayList<String> aListOfValues = new ArrayList<String>( Arrays.asList(attrValues) );
			Tuple tuple = relation_reference.createTuple();
			
			
			for(int i=0; i<attrNames.length; i++)
			{
				try
				{
					int val = Integer.parseInt(attrValues[i]);
					tuple.setField(attrNames[i], val);
				}
				catch(NumberFormatException ex)
				{
					tuple.setField(attrNames[i], attrValues[i]);
				}
				//tuple.setField(attrNames[i],"v11");			    
			}
			valuesList.add(tuple);
		}
		
		//parsing logic ends 
		int count = 0;
		//now attributes names and values have been collected.Lets create a tuple
		for( int j = 0; j< valuesList.size(); j++)
		{
			count++;
			Tuple current = valuesList.get(j);
						
			Block block_reference=mem.getBlock(0); //access to memory block 0
		    block_reference.clear(); //clear the block
		    
		    block_reference.appendTuple(current);		    
			
		    Parser.appendTupleToRelation(relation_reference, mem, 9, current);
		}
			    
	   if(count > 1)
	   {
		   //System.out.println(count+" rows inserted successfully."); 
		   //System.out.println("No of disk I/Os for this operation are " + disk.getDiskIOs());
	   }		  
	   else
	   {
		   //System.out.println(count+" row inserted successfully.");
		   //System.out.println("No of disk I/Os for this operation are " + disk.getDiskIOs());
	   }
		   
		return true;
	}
}