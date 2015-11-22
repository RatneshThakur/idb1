package sqlEngine;

import java.util.ArrayList;

import storageManager.Disk;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.SchemaManager;

public class CreateStatement
{
	MainMemory mem;
    Disk disk;   
    SchemaManager schema_manager;
	
	String stmt;
	ArrayList<String> attr_List;
	ArrayList<FieldType> attr_types;	
	
	public CreateStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		stmt = stmt_var;
		mem = mem_var;
		disk = disk_var;
	    schema_manager =schema_manager_var;
	    
	    attr_List = new ArrayList<String>();
		attr_types = new ArrayList<FieldType>();
	}
	
	public Relation runStatement()
	{
		String tableName = stmt.split(" ")[2];
		getAttributesNameType();
		
		//System.out.println(" Statement is  " + stmt);
		
//		for(int i=0; i<attr_List.size(); i++)
//		{
//			System.out.println(" " + attr_List.get(i) + " " + attr_types.get(i));
//		}
		Schema schema = new Schema(attr_List,attr_types);
		
		String relation_name=tableName;
	    Relation relation_reference=schema_manager.createRelation(relation_name,schema);		
		
	    System.out.println(" " + relation_name + " created successfully ");
		
		return relation_reference;
	}
	
	private void getAttributesNameType()
	{
		String result = stmt.substring(stmt.indexOf("(") + 1, stmt.indexOf(")"));
		
		String[] resultSplit = result.split(",");
		for(int i=0; i<resultSplit.length; i++)
		{
			String newString = resultSplit[i].trim();
			String[] nameType = newString.split(" ");
			attr_List.add(nameType[0]);
			
			if(nameType[1].equals("INT"))
			{				
				attr_types.add(FieldType.INT);
			}
			else
				attr_types.add(FieldType.STR20);
				
		}
	}
	
	
}