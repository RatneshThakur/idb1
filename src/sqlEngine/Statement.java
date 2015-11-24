package sqlEngine;

import java.util.ArrayList;
import java.util.Comparator;
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
	
	public String getTableNames(ArrayList<String> tableNames,String pattern1, String pattern2,ArrayList<String> orderByList)
	{
		
		// for statements like select something/* from table name where somecondition
		
		Matcher m = null;
		Pattern p = null;
		String whereCondition = "";
		if(stmt.contains("WHERE"))
		{
			//pattern1 = FROM
			//pattern2 = WHERE
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
			
			if(stmt.contains("ORDER BY"))
			{
				//pattern1 = "WHERE";
				pattern2 = "ORDER BY";				
				
				p = Pattern.compile(Pattern.quote(pattern2) + "(.*)");
				m = p.matcher(stmt);
				String orderByCols = "";
				if ( m.find() ) {
					orderByCols = m.group(1);
				}
				String[] orderByColsSplit = orderByCols.split(",");
				for(int i=0; i<orderByColsSplit.length; i++)
				{
					orderByList.add(orderByColsSplit[i].trim());
				}
				
				//getting where clause
				pattern1 = "WHERE";
				pattern2 = "ORDER BY";
				p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
				m = p.matcher(stmt);				
				while(m.find())
				{
					whereCondition = m.group(1);;				
				}
				
			}
			else{
				// no order by clause
				p = Pattern.compile(Pattern.quote(pattern2) + "(.*)");
				m = p.matcher(stmt);			
				if ( m.find() ) {
					
					whereCondition = m.group(1);
				}	
			}
			
			for(String s : orderByList)
				System.out.println(" order by column " + s);
					
		} // end of where if
		else
		{
			//this is for statements like select something/*  from tablename ... no condition   "sentence(.*)"
			// will check for order by here
			
			if(stmt.contains("ORDER BY"))
			{
				pattern1 = "FROM";
				pattern2 = "ORDER BY";
				p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
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
				
				System.out.println(" order by certain column is needed ");
				
				p = Pattern.compile(Pattern.quote(pattern2) + "(.*)");
				m = p.matcher(stmt);
				String orderByCols = "";
				if ( m.find() ) {
					orderByCols = m.group(1);
				}
				String[] orderByColsSplit = orderByCols.split(",");
				for(int i=0; i<orderByColsSplit.length; i++)
				{
					orderByList.add(orderByColsSplit[i].trim());
				}				
				
			}
			else{
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
		//precedence is according to oracle site.
		// https://docs.oracle.com/cd/B19188_01/doc/B15917/sqopr.htm
		opMap.put("/", 5);
		opMap.put("*", 5);
		opMap.put("+", 4);
		opMap.put("-",4);
		opMap.put("=",3);
		opMap.put(">", 3);
		opMap.put("<", 3);
		opMap.put("NOT",2);
		opMap.put("AND",1);
		opMap.put("OR",0);
		
		
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
		
		boolean stringCompare = false;
		
		field2 = field2.substring(field2.lastIndexOf(".") + 1);
		field1 = field1.substring(field1.lastIndexOf(".") + 1);
		
		if(current.getSchema().fieldNameExists(field1))
		{
			if(current.getField(field1).type == FieldType.INT)
			{
				value1 = current.getField(field1).integer;
			}
			else if( current.getField(field1).type == FieldType.STR20)
			{
				stringValue1 = current.getField(field1).str;
				stringCompare = true;
			}
		}
		else
		{
			try{
				value1 = Integer.parseInt(field1);
			}
			catch(NumberFormatException ex)
			{
				stringValue1 = field1;
				stringCompare = true;
			}
			
			
		}
			
		if(current.getSchema().fieldNameExists(field2))
		{
			if(current.getField(field2).type == FieldType.INT)
			{
				value2 = current.getField(field2).integer;
			}
			else if( current.getField(field2).type == FieldType.STR20)
			{
				stringValue2 = current.getField(field2).str;
				stringCompare = true;
			}
		}
		else
		{
			try{
				value2 = Integer.parseInt(field2);
			}
			catch(NumberFormatException ex)
			{
				stringValue2 = field2;
				stringCompare = true;
			}
			
		}
		
		if(operator.equals(">"))
			output.push(new Boolean(value2>value1).toString());
		else if(operator.equals("=") && (stringCompare == false))
			output.push(new Boolean(value2 == value1).toString());
		else if(operator.equals("=") && (stringCompare == true))
			output.push(new Boolean(stringValue2.equals(stringValue1)).toString());
		else if(operator.equals("<"))			
			output.push(new Boolean(value2<value1).toString());			
		else
			output.push(new Boolean(false).toString());
		
	}
	public void numberEvaluate(Tuple current, String field1, String field2, Stack<String> output, String operator)
	{
		int value1 = 0;
		int value2 = 0;
		field2 = field2.substring(field2.lastIndexOf(".") + 1);
		field1 = field1.substring(field1.lastIndexOf(".") + 1);
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
		
		//precedence is according to oracle site. Higher no - higher priority
		// https://docs.oracle.com/cd/B19188_01/doc/B15917/sqopr.htm
		 
		opMap.put("/", 5);
		opMap.put("*", 5);
		opMap.put("+", 4);
		opMap.put("-",4);
		opMap.put("=",3);
		opMap.put(">", 3);
		opMap.put("<", 3);
		opMap.put("NOT",2);
		opMap.put("AND",1);
		opMap.put("OR",0);
		
		
		
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

class MyComparator implements Comparator<Tuple>
{
	private String field;
	MyComparator(String field_var)
	{
		field = field_var;
	}
	public int compare(Tuple t1, Tuple t2)
	{
		int val1;
		int val2;
		String stringVal1;
		String stringVal2;
		//int fields
		if(t1.getField(field).type == FieldType.INT)
		{
			val1 = t1.getField(field).integer;
			val2 = t2.getField(field).integer;
			return Integer.compare(val1, val2);
		}
		if(t1.getField(field).type == FieldType.STR20)
		{
			stringVal1 = t1.getField(field).str;
			stringVal2 = t2.getField(field).str;
			return stringVal1.compareTo(stringVal2);
		}
		//will never reach here
		return -1;
	}
}