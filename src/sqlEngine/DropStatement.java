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
		
		if(relation_name != null)
		{
			if(schema_manager.deleteRelation(relation_name) == true);
			{
				//System.out.println("Relation '" + relation_name + "' deleted successfully.");
			}
		}
		

		return true;
	}
}