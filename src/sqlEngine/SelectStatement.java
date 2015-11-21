package sqlEngine;

import java.util.*;
import java.util.HashMap;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storageManager.Block;
import storageManager.Disk;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.SchemaManager;
import storageManager.Tuple;

public class SelectStatement extends Statement
{
	//Inherited member variables are in comments below
	//MainMemory mem;
    //Disk disk;   
    //SchemaManager schema_manager;
    //String stmt;
	
	
	Relation relation_reference;
	String relation_name;
	
	public SelectStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		super(stmt_var,mem_var,disk_var,schema_manager_var);
	}
	
	public ArrayList<ArrayList<String>> runStatement(boolean isPartOfQuery)
	{
		//parsing logic starts here
		String pattern1 = "SELECT";
		
		String pattern2 = "FROM";
		
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> tableNames = new ArrayList<String>();
		
		ArrayList<String> attrList = new ArrayList<String>();
		
		String whereCondition = "";

		Pattern p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
		Matcher m = p.matcher(stmt);
		while (m.find()) {
			String col = m.group(1);
			columnNames.add(col);		  
		}
		
		boolean isPrintAllColumns = false;
		
		if(columnNames.size() == 1 && columnNames.get(0).trim().equals("*"))
		{			
			isPrintAllColumns = true;
		}
		else
		{
			String[] colSplit = columnNames.get(0).split(",");
			for(int i=0; i< colSplit.length; i++)
			{
				attrList.add(colSplit[i].trim());
			}
		}
		
		
		//getting all table names now
		pattern1 = "FROM";
		pattern2 = "WHERE";
		
		whereCondition = getTableNames(tableNames,pattern1,pattern2);
				
		//parsing logic ends here
		
		//following code is assumed for only one table name in from.. Later need to expand it to join 
		relation_reference = schema_manager.getRelation(tableNames.get(0));
		
		//was getting everything in once
		//relation_reference.getBlocks(0,3,relation_reference.getNumOfBlocks());
	    
		
		ArrayList<String> fieldNames = new ArrayList<String>();
		if(isPrintAllColumns == true)
		 fieldNames = relation_reference.getSchema().getFieldNames();
		else
		  fieldNames = attrList;
		
		if( isPartOfQuery == false)
		{
			for( int i=0; i<fieldNames.size(); i++)
			{
				System.out.print(fieldNames.get(i));
				System.out.print("   |  ");
			}
			System.out.println(" ");
			System.out.println("----------------------------------------------------");
		}
		
		
		ArrayList<ArrayList<String>> output = new ArrayList<ArrayList<String>>();
		
		ArrayList<String> aListOfValues = new ArrayList<String>();
		
		for(int i=0; i<relation_reference.getNumOfBlocks(); i++)
		{			
			relation_reference.getBlock(i,3);
			
			Block block_reference=mem.getBlock(3);
			Tuple current = block_reference.getTuple(0);
			
			if(testCondition(current,whereCondition) == true)
			{
				for(int j=0; j<fieldNames.size(); j++)
				{
					if(isPartOfQuery == false)
						System.out.print("\t" + current.getField(fieldNames.get(j)));
					else
					{
						if(current.getField(fieldNames.get(j)).type == FieldType.INT)
						{
							aListOfValues.add(new Integer( current.getField(fieldNames.get(j)).integer).toString());
						}
						else if(current.getField(fieldNames.get(j)).type == FieldType.STR20)
						{
							aListOfValues.add(new String( current.getField(fieldNames.get(j)).str));
						}
					}
				}
				if( isPartOfQuery == false)
					System.out.println(" ");
				else
				{
					output.add(aListOfValues);
					aListOfValues = new ArrayList<String>();
				}
			}
			
		}
		
		if(aListOfValues.size() > 0)
		{
			output.add(aListOfValues);
		}
		
		return output;
	}	
	
	
}