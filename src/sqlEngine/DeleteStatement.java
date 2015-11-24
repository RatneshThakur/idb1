package sqlEngine;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storageManager.Block;
import storageManager.Disk;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.SchemaManager;
import storageManager.Tuple;

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
		//getting all table names now
				String pattern1 = "FROM";
				String pattern2 = "WHERE";
				ArrayList<String> tableNames = new ArrayList<String>();
				
				ArrayList<String> orderByList = new ArrayList<String>();
				//here there is no use of orderbylist variable. but since we need the variable in function defition
				//that is why i am passing the variable
				
				String whereCondition = getTableNames(tableNames,pattern1,pattern2,orderByList);
						
				//parsing logic ends here
				
				relation_reference = schema_manager.getRelation(tableNames.get(0));
				
				//was getting everything in once
				//relation_reference.getBlocks(0,3,relation_reference.getNumOfBlocks());
				//System.out.print("Relation state before deletion: " + "\n");
			    //System.out.print(relation_reference + "\n");
			    
				
				ArrayList<String> fieldNames = new ArrayList<String>();				
				int count = 0;
				for(int i=0; i<relation_reference.getNumOfBlocks(); i++)
				{			
					relation_reference.getBlock(i,3);					
					Block block_reference=mem.getBlock(3);
					
					for( int j=0; j< block_reference.getNumTuples(); j++)
					{
						Tuple current = block_reference.getTuple(j);
						
						if(current.isNull() == false && testCondition(current,whereCondition) == true)
						{	
							count++;
							block_reference.invalidateTuple(j);	
							
						}
					}
					relation_reference.setBlock(i,3);
				}
				
				System.out.println(" " + count + " records/tuples deleted successfully ");
				//System.out.print("Relation state after deletion: " + "\n");
			    //System.out.print(relation_reference + "\n");
		
	}
}

