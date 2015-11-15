package sqlEngine;

import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.SchemaManager;

class DropStatement extends Statement
{
	Relation relation_reference;
	String relation_name;
	public DropStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		super(stmt_var,mem_var,disk_var,schema_manager_var);
	}
	
	public boolean runStatement()
	{
		relation_name = stmt.split(" ")[2];
		System.out.println("Schema manager before deletion " + schema_manager);
		
		if(relation_name != null)
			schema_manager.deleteRelation(relation_name);
		
		System.out.println(" Successfull drop of the table ");
		System.out.println("Schema manager after deletion " + schema_manager);
		

		return true;
	}
}