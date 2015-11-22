package sqlEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storageManager.Disk;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.SchemaManager;
import storageManager.Tuple;

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
			selectStmt.runStatement(false);
		}
		else if(stmtSplit[0].equals("DELETE"))
		{
			DeleteStatement deleteStmt = new DeleteStatement(stmt,mem,disk,schema_manager);
			deleteStmt.runStatement();
		}
		
	}
	
	public String getTableNames(ArrayList<String> tableNames,String pattern1, String pattern2)
	{
		
		// for statements like select something/* from table name where somecondition
		Matcher m = null;
		Pattern p = null;
		String whereCondition = "";
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
		return whereCondition;
	}
	
	public boolean testCondition(Tuple current, String whereClause)
	{
		if(whereClause.length() == 0)
			return true;
		
		ArrayList<String> postFix = createPostFix(whereClause);		
		
		
		if(postFix.size() == 0)
			return true;
		HashMap<String, Integer> opMap = new HashMap<String,Integer>();
		opMap.put("+", 2);
		opMap.put("/", 2);
		opMap.put("*", 2);
		opMap.put("-",2);
		opMap.put("=",1);
		opMap.put("AND",0);
		opMap.put("OR",0);
		opMap.put(">", 1);
		opMap.put("<", 1);
		opMap.put("NOT",0);
		
		Stack<String> output = new Stack<String>();
		
		for(int i=0; i<postFix.size(); i++)
		{
			if(opMap.containsKey(postFix.get(i)))
			{
				String field1 = "";
				String field2 = "";
				if(postFix.get(i).equals("NOT"))
				{
					//will ignore the field2 in the function
					field1 = output.pop();
					booleanLogicalEvaluate(current, field1,field2, output, postFix.get(i));
					
				}					
				else{
					field1 = output.pop();
					field2 = output.pop();	
				}
								
				
				if(postFix.get(i).equals("+"))
				{					
					numberEvaluate(current, field1, field2, output, postFix.get(i));					
				}
				else if(postFix.get(i).equals("-"))
				{						
					numberEvaluate(current, field1, field2, output, postFix.get(i));					
				}
				else if(postFix.get(i).equals("*"))
				{						
					numberEvaluate(current, field1, field2, output, postFix.get(i));					
				}
				else if(postFix.get(i).equals("/"))
				{						
					numberEvaluate(current, field1, field2, output, postFix.get(i));					
				}
				else if(postFix.get(i).equals("="))
				{
					booleanEvaluate(current, field1, field2, output, postFix.get(i));		
				}
				else if(postFix.get(i).equals(">"))
				{						
					booleanEvaluate(current, field1, field2, output, postFix.get(i));
				}
				else if(postFix.get(i).equals("<"))
				{					
					booleanEvaluate(current, field1, field2, output, postFix.get(i));
				}
				else if(postFix.get(i).equals("AND"))
				{
					booleanLogicalEvaluate(current, field1, field2, output, postFix.get(i));
				}
				else if(postFix.get(i).equals("OR"))
				{
					booleanLogicalEvaluate(current, field1, field2, output, postFix.get(i));
				}
				
			}
			else
				output.push(postFix.get(i));
		}
		
		return new Boolean(output.pop());
		
	}
	
	public void booleanLogicalEvaluate(Tuple current, String field1, String field2, Stack<String> output, String operator)
	{
		boolean value1;
		boolean value2;
		
		try{
		value1 = Boolean.parseBoolean(field1);
		value2 = Boolean.parseBoolean(field2);		
		
		if(operator.equals("AND"))
			output.push(new Boolean(value2&&value1).toString());
		else if(operator.equals("OR"))
			output.push(new Boolean(value2||value1).toString());
		else if(operator.equals("NOT"))
			output.push(new Boolean(!value1).toString());
		else
			output.push(new Boolean(false).toString());
		}
		catch(Exception ex)
		{
			output.push(new Boolean(false).toString());
		}
	}
	
	public void booleanEvaluate(Tuple current, String field1, String field2, Stack<String> output, String operator)
	{
		
		int value2 = 0;
		int value1 = 1;
		
		String stringValue1 = "";
		String stringValue2 = "";
		
		boolean isFieldString = false;
		
		if(current.getSchema().fieldNameExists(field2))
		{
			if(current.getField(field2).type == FieldType.INT)
			{
				isFieldString = false;
				value2 = current.getField(field2).integer;
			}
			else if( current.getField(field2).type == FieldType.STR20 )
			{
				isFieldString = true;
				stringValue2 = current.getField(field2).str;
			}
		}		
		
		
		if(isFieldString == false)
			value1 = Integer.parseInt(field1);
		else
			stringValue1 = field1;
		
			
		
		
		if(operator.equals(">"))
			output.push(new Boolean(value2>value1).toString());
		else if(operator.equals("=") && (isFieldString == false))
			output.push(new Boolean(value2 == value1).toString());
		else if(operator.equals("=") && (isFieldString == true))
			output.push(new Boolean(stringValue1.equals(stringValue2)).toString());
		else if(operator.equals("<"))			
			output.push(new Boolean(value2<value1).toString());			
		else
			output.push(new Boolean(false).toString());
		
	}
	public void numberEvaluate(Tuple current, String field1, String field2, Stack<String> output, String operator)
	{
		int value1 = 0;
		int value2 = 0;
		if(current.getSchema().fieldNameExists(field1))
		{
			if(current.getField(field1).type == FieldType.INT)
			{
				value1 = current.getField(field1).integer;
			}
		}
		else
			value1 = Integer.parseInt(field1);
		if(current.getSchema().fieldNameExists(field2))
		{
			if(current.getField(field2).type == FieldType.INT)
			{
				value2 = current.getField(field2).integer;
			}
		}
		else
		{
			value2 = Integer.parseInt(field2);
		}
		
		if(operator.equals("+"))
			output.push(new Integer(value2 + value1).toString());
		else if(operator.equals("-"))
			output.push(new Integer(value2 - value1).toString());
		else if(operator.equals("*"))
			output.push(new Integer(value2 * value1).toString());
		else if(operator.equals("/"))
			output.push(new Integer(value2 / value1).toString());
		else
			output.push(new Integer(value1).toString());
	}
	
	public ArrayList<String> createPostFix(String whereClause)
	{
		String[] tokens = whereClause.split(" ");
		Stack<String> opStack = new Stack<String>();
		
		ArrayList<String> postFix = new ArrayList<String>();
		
		HashMap<String, Integer> opMap = new HashMap<String,Integer>();
		opMap.put("+", 2);
		opMap.put("/", 2);
		opMap.put("*", 2);
		opMap.put("-",2);
		opMap.put("=",1);
		opMap.put("AND",0);
		opMap.put("OR",0);
		opMap.put(">", 1);
		opMap.put("<", 1);
		opMap.put("NOT",0);
		
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