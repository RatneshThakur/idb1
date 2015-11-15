package sqlEngine;

import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;

class Statement
{
	MainMemory mem;
    Disk disk;   
    SchemaManager schema_manager;
    
	String stmt;
	
	public Statement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		stmt = stmt_var;
		mem = mem_var;
		disk = disk_var;
	    schema_manager =schema_manager_var;
	}
	
	public void analyzeStatement()
	{
		String[] stmtSplit = stmt.split(" ");
		System.out.println(" The statment is : " + stmt);
		// will optimize it later -- Need to remove split 
	
		if(stmtSplit[0].equals("CREATE")){
			CreateStatement cs = new CreateStatement(stmt,mem,disk,schema_manager);
			cs.runStatement();
		}
		else if(stmtSplit[0].equals("INSERT"))
		{
			InsertStatement insertStmt = new InsertStatement(stmt,mem,disk,schema_manager);
			insertStmt.runStatement();
			//This is also done.
		}
		else if(stmtSplit[0].equals("DROP"))
		{
			DropStatement dropStmt = new DropStatement(stmt,mem,disk,schema_manager);
			dropStmt.runStatement();
		}
		else if(stmtSplit[0].equals("SELECT"))
		{
			SelectStatement selectStmt = new SelectStatement(stmt,mem,disk,schema_manager);
			selectStmt.runStatement();
		}
		else if(stmtSplit[0].equals("DELETE"))
		{
			DeleteStatement deleteStmt = new DeleteStatement(stmt,mem,disk,schema_manager);
			deleteStmt.runStatement();
		}
		
	}
}