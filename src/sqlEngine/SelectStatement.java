package sqlEngine;

import java.util.ArrayList;
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
	
	public boolean runStatement()
	{
		//parsing logic starts here
		String pattern1 = "SELECT";
		
		String pattern2 = "FROM";
		
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> tableNames = new ArrayList<String>();
		
		String whereCondition = "";

		Pattern p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
		Matcher m = p.matcher(stmt);
		while (m.find()) {
			String col = m.group(1);
			columnNames.add(col);		  
		}
		
		boolean isPrintAllColumns = false;
		if(columnNames.size() == 1 && columnNames.get(0).equals("*"))
		{
			isPrintAllColumns = true;
			System.out.println("Print all columns");
		}
		
		//getting all table names now
		pattern1 = "FROM";
		pattern2 = "WHERE";
		// for statements like select something/* from tablname where somecondition
		if(stmt.contains("WHERE"))
		{
			p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
			m = p.matcher(stmt);
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
		else
		{
			//this is for statements like select something/*  from tablename ... no condition   "sentence(.*)"
			p = Pattern.compile(Pattern.quote(pattern1) + "(.*)");
			m = p.matcher(stmt);
			String allTables = "";
			if ( m.find() ) {
			   allTables = m.group(1);
			}
			String[] allTablesSplit = allTables.split(",");
			for(int i=0; i<allTablesSplit.length; i++)
			{
				tableNames.add(allTablesSplit[i].trim());
			}			
		}
		
		System.out.println(" Table name is " + tableNames.get(0));
		
		//parsing logic ends here
		
		//following code is assumed for only one table name in from.. Later need to expand it to join 
		relation_reference = schema_manager.getRelation(tableNames.get(0));
		
		//was getting everything in once
		//relation_reference.getBlocks(0,3,relation_reference.getNumOfBlocks());
	    
		
		System.out.println( " Column names " + relation_reference.getSchema().getFieldNames());
		ArrayList<String> fieldNames = new ArrayList<String>();
		if(isPrintAllColumns == true)
		 fieldNames = relation_reference.getSchema().getFieldNames();
		for( int i=0; i<fieldNames.size(); i++)
		{
			System.out.print(fieldNames.get(i));
			System.out.print("   |  ");
		}
		System.out.println(" ");
		System.out.println("----------------------------------------------------");
		
		
		for(int i=0; i<relation_reference.getNumOfBlocks(); i++)
		{			
			relation_reference.getBlock(i,3);
			
			Block block_reference=mem.getBlock(3);
			Tuple current = block_reference.getTuple(0);
			
			if(testCondition(current,whereCondition) == true)
				System.out.println(current);			
			
			
		}
		
		
	    
		return true;
	}
	private boolean testCondition(Tuple current, String whereClause)
	{
		
		ArrayList<String> postFix = createPostFix(whereClause);		
		
		
		if(postFix.size() == 0)
			return true;
		HashMap<String, Integer> opMap = new HashMap<String,Integer>();
		opMap.put("+", 2);
		opMap.put("/", 2);
		opMap.put("*", 2);
		opMap.put("-",2);
		opMap.put("=",1);
		opMap.put("AND",1);
		opMap.put("OR",1);
		opMap.put(">", 1);
		
		Stack<String> output = new Stack<String>();
		
		for(int i=0; i<postFix.size(); i++)
		{
			if(opMap.containsKey(postFix.get(i)))
			{
				String field1 = output.pop();
				String field2 = output.pop();
				
				if(postFix.get(i).equals("+"))
				{
					Integer result = current.getField(field1).integer + current.getField(field2).integer;
					output.push(result.toString());
				}
				else if(postFix.get(i).equals("="))
				{
					if( current.getSchema().fieldNameExists(field2) )
					{
						if(current.getField(field2).type == FieldType.INT)
						{
							output.push(new Boolean(current.getField(field2).integer == Integer.parseInt(field1)).toString());
						}
						
					}
					
				}			
			}
			else
				output.push(postFix.get(i));
		}
		
		return new Boolean(output.pop());
		
	}
	private ArrayList<String> createPostFix(String whereClause)
	{
		String[] tokens = whereClause.split(" ");
		Stack<String> opStack = new Stack<String>();
		
		ArrayList<String> postFix = new ArrayList<String>();
		
		HashMap<String, Integer> opMap = new HashMap<String,Integer>();
		opMap.put("+", 2);
		opMap.put("/", 2);
		opMap.put("*", 2);
		opMap.put("-",1);
		opMap.put("=",1);
		opMap.put("AND",1);
		opMap.put("OR",1);
		opMap.put(">", 1);
		
		for(int i=0; i<tokens.length; i++)
		{
			if((opMap.containsKey(tokens[i]) == false) && !(tokens[i].equals("[")) && !(tokens[i].equals("]")) )
			{
				postFix.add(tokens[i]);
			}
			else if(tokens[i].equals("["))
			{
				opStack.push(tokens[i]);
			}
			else if(opMap.containsKey(tokens[i]) == true)
			{
				while((opStack.size() > 0 ) && !(opStack.peek().equals("[")))
				{
					String topOp = opStack.peek();
					if( opMap.get(topOp) > opMap.get(tokens[i]))
					{
						postFix.add(opStack.pop());
					}
					else
						break;
				}
				opStack.push(tokens[i]);
			}
			else if(tokens[i].equals("]"))
			{
				while ((opStack.size() > 0) && !(opStack.peek().equals("[")))
	            {
					postFix.add(opStack.pop());
	            }
	            if (opStack.size() > 0)
	                opStack.pop(); // popping out the left brace '('				
			}
		}
		
		while(opStack.size() > 0)
			postFix.add(opStack.pop());		
		
	
		return postFix;
	}
}