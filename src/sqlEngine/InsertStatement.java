package sqlEngine;

import java.util.ArrayList;
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
	public InsertStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		super(stmt_var,mem_var,disk_var,schema_manager_var);
	}
	
	//Insert Statement
	public boolean runStatement()
	{
		
		String[] attrNames = null; // this will hold attribute names sid, homework etc
		String[] attrValues = null; // this will hold attribute values;
		relation_name = stmt.split(" ")[2];
		relation_reference = schema_manager.getRelation(relation_name);
		// INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, "A")
		
		//parsing logic starts
		stmt = stmt.replace('(', '{');
		stmt = stmt.replace(')', '}');
		
		List<String> allMatches = new ArrayList<String>();
		 Matcher m = Pattern.compile("\\{([^}]*)\\}")
		     .matcher(stmt);
		 while (m.find()) {
		   String matchedPart = m.group();
		   matchedPart= matchedPart.substring(1, matchedPart.length()-1);
		   allMatches.add(matchedPart);		   
		 }	
		
		attrNames = allMatches.get(0).split(",");
		attrValues = allMatches.get(1).split(",");		
		//removing extra spaces if any
		for(int i=0; i<attrNames.length; i++)
		{
			attrNames[i] = attrNames[i].trim();
			attrValues[i] = attrValues[i].trim();
		}		 
		//parsing logic ends 
		
		//now attributes names and values have been collected.Lets create a tuple
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
		
		Block block_reference=mem.getBlock(0); //access to memory block 0
	    block_reference.clear(); //clear the block
	    
	    block_reference.appendTuple(tuple);
	    
		
	    Parser.appendTupleToRelation(relation_reference, mem, 9, tuple);	    
	   
		return true;
	}
}