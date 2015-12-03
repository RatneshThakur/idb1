package sqlEngine;

import java.io.*;
import java.util.*;
import java.util.HashMap;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.plaf.synth.SynthOptionPaneUI;

import storageManager.Block;
import storageManager.Disk;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
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
	
	public SelectStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var, PrintWriter writer_var)
	{
		super(stmt_var,mem_var,disk_var,schema_manager_var,writer_var);
	}
	
	public ArrayList<Tuple> runStatement(boolean isPartOfQuery)
	{
		//parsing logic starts here
		String pattern1 = "SELECT";		
		String pattern2 = "FROM";
		
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> tableNames = new ArrayList<String>();
		ArrayList<String> orderByList = new ArrayList<String>();
		
		ArrayList<String> attrList = new ArrayList<String>();		
		ArrayList<String> distinctAttrList = new ArrayList<String>();
		
		boolean distinctPresent = false;
		boolean isOrderByPresent = false;
		boolean isPerformJoin = false;
		boolean isPrintAllColumns = false;
		
		if(stmt.contains("ORDER BY"))
			isOrderByPresent = true;
		
		String whereCondition = "";

		Pattern p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
		Matcher m = p.matcher(stmt);
		while (m.find()) {
			String col = m.group(1);
			columnNames.add(col);		  
		}
		
		
		if(columnNames.size() == 1 && columnNames.get(0).trim().contains("*"))
		{			
			isPrintAllColumns = true;
			if(columnNames.get(0).trim().contains("DISTINCT"))
			{				
				distinctPresent = true;
			}
		}
		else
		{
			String[] colSplit = columnNames.get(0).split(",");
			
			for(int i=0; i< colSplit.length; i++)
			{
				
				if( colSplit[i].contains("DISTINCT"))
				{
					colSplit[i] = colSplit[i].substring(colSplit[i].lastIndexOf(".") + 1); 	// for removing course.grade == course and grade
					
					if(colSplit[i].trim().split(" ").length == 2)
						colSplit[i] = colSplit[i].trim().split(" ")[1];
					else
						colSplit[i] = colSplit[i].trim().split(" ")[0];
					distinctAttrList.add(colSplit[i].trim());
					attrList.add(colSplit[i].trim());
					distinctPresent = true;
				}
				else{
					colSplit[i] = colSplit[i].substring(colSplit[i].lastIndexOf(".") + 1);
					attrList.add(colSplit[i].trim());
				}
					
			}
		}
		
		//attrList contains all the attributes which will be used for projection.
		//distinctAttrList contains all the attributes which will be used for distinctiness.
		
		//getting all table names now
		pattern1 = "FROM";
		pattern2 = "WHERE";
		
		// we get 
		//1. tablesNames
		//2. Order by list of attributes
		
		whereCondition = getTableNames(tableNames,pattern1,pattern2, orderByList);
		ArrayList<Tuple> outputTuplesList = new ArrayList<Tuple>();
		
		ArrayList<String> fieldNames = new ArrayList<String>();
		
		fieldNames = attrList;
		
		if(tableNames.size() > 1)
		{	
			
			isPerformJoin = true;
			fieldNames = getProjectionAttributes(true, tableNames, distinctAttrList);
			//from here
			String[] tableNamesArray = new String[tableNames.size()];
			tableNames.toArray(tableNamesArray);
			ArrayList<Tuple> table1Data = null;
			ArrayList<Tuple> table2Data = null;
			if(tableNames.size() == 2)
			{	
				if(checkIfValid(tableNames, "join") == false)
				{
					System.out.println("error");
					writer.println("error");
					return new ArrayList<Tuple>();
				}
				table1Data = outputTuplesList = performTwoPassJoin(tableNames, whereCondition, table1Data, table2Data,false);
			}
			else
			{
				table1Data = getTableData(tableNamesArray[0]);
				table2Data = getTableData(tableNamesArray[1]);
				table1Data = outputTuplesList = performTwoPassJoin(tableNames, whereCondition, table1Data, table2Data, false);
				tableNamesArray[1] = tableNamesArray[0]+"join"+tableNamesArray[1];
				
				for(int i=2; i<tableNamesArray.length; i++)
				{
					
					table2Data = getTableData(tableNamesArray[i]);
					ArrayList<String> twoTablesList = new ArrayList<String>();
					twoTablesList.add(tableNamesArray[i-1]);
					twoTablesList.add(tableNamesArray[i]);
					for(int t=0; t<5;t++)
					{
						getTableData(tableNamesArray[0]);
					}
					outputTuplesList = table1Data = performTwoPassJoin(twoTablesList, whereCondition, table1Data, table2Data, true);	
					tableNamesArray[i] = tableNamesArray[i-1]+"join"+tableNamesArray[i]; 
				}
			}
			
			
			
			//joinCleanup(tableNames);
			//till here
			
			//outputTuplesList = performTwoPassJoin(tableNames, whereCondition, isOrderByPresent, fieldNames);
			
			//outputTuplesList = performJoinaAndOutput(tableNames,whereCondition, fieldNames);			
			//return outputTuplesList;
		}
				
		//parsing logic ends here
		
		//following code is assumed for only one table name in from.. Later need to expand it to join 
		relation_reference = schema_manager.getRelation(tableNames.get(0));
		
		if(relation_reference == null)
		{
			System.out.println(tableNames.get(0) + " does not exist.");
			return new ArrayList<Tuple>();
		}
		//test twoPasssort algorithms
		//twoPassSort(relation_reference);
		//onePassSort(relation_reference);
		
		//was getting everything in once
		//relation_reference.getBlocks(0,3,relation_reference.getNumOfBlocks());
	    
	
		
		
		if(isPerformJoin == false)
		{
			if(isPrintAllColumns == true)
				 fieldNames = relation_reference.getSchema().getFieldNames();
			for(int i=0; i<relation_reference.getNumOfBlocks(); i++)
			{			
				relation_reference.getBlock(i,3);
				
				Block block_reference=mem.getBlock(3);
				
				int numOfTuples = block_reference.getNumTuples();
				for(int k=0; k<numOfTuples; k++)
				{
					Tuple current = block_reference.getTuple(k);				
					if(current.isNull() == true)
						numOfTuples++;				
					else if(testCondition(current,whereCondition) == true)
					{
						outputTuplesList.add(current);					
					}				
				}
			}
		}
		
		
		
		if(distinctPresent == true)
		{
			if(isPerformJoin == true)
			{
				distinctAttrList = new ArrayList<String>();
				fieldNames = getProjectionAttributes(true, tableNames, distinctAttrList);
			}
			else
			{
				if(checkIfValid(tableNames,"distinct") == false)
				{
					System.out.println("error");
					writer.println("error");
					return new ArrayList<Tuple>();
				}
			}
				
			if(isPrintAllColumns == true )
				distinctAttrList = fieldNames;
			if(isPerformJoin == false)
				outputTuplesList = duplicateElimination(outputTuplesList, distinctAttrList, fieldNames,false,tableNames);	
			else
				outputTuplesList = duplicateElimination(outputTuplesList, distinctAttrList,fieldNames,true,tableNames);
		}
		if(isOrderByPresent == true)
		{
			if(checkIfValid(tableNames,"orderby") == false)
			{
				System.out.println("error");
				writer.println("error");
				return new ArrayList<Tuple>();
			}
			
			sortTuplesByColumn(outputTuplesList,orderByList,isPerformJoin, tableNames);
		}		
		
		if( isPartOfQuery == false)
		{
			if(isPerformJoin == true)
				fieldNames = getProjectionAttributes(true, tableNames, distinctAttrList);
		
			projectionTuples(outputTuplesList, fieldNames, whereCondition);			
		}
			
		joinCleanup(tableNames);
		return outputTuplesList;
	}
	
	public ArrayList<Tuple> getTableData(String tableName)
	{
		Relation relation = schema_manager.getRelation(tableName);
		ArrayList<Tuple> relationList = new ArrayList<Tuple>();
		for(int i=0; i<relation.getNumOfBlocks(); i++)
		{
			mem.getBlock(0).clear();
			relation.getBlock(i,0);			
			ArrayList<Tuple> t1List = mem.getTuples(0,1);
			for(int t=0; t<t1List.size(); t++)
			{
				relationList.add(t1List.get(t));
			}
		}
		
		return relationList;
	}
	
	private void sortTuplesByColumn(ArrayList<Tuple> outputTuples, ArrayList<String> orderByList, boolean isJoin, ArrayList<String> tableNames)
	{
		String field = orderByList.get(0);
		if(isJoin == false)
		{	//getTableData(outputTuples.get(0).getSchema().g)
			field = field.substring(field.lastIndexOf(".") + 1);
			int block = schema_manager.getRelation(tableNames.get(0)).getNumOfBlocks();
			
			if(block > 9)
			{
				getTableData(tableNames.get(0));
				getTableData(tableNames.get(0));
			}
			
		}
		
			
		ArrayList<String> temp = outputTuples.get(0).getSchema().getFieldNames();
		Tuple t = outputTuples.get(0);		
		Collections.sort(outputTuples, new MyComparator(field));
	}
	
	private ArrayList<Tuple> duplicateElimination( ArrayList<Tuple> outputTuples, ArrayList<String> distinctAttrs, ArrayList<String> fieldNames, boolean isJoin, ArrayList<String> tableNames)
	{
		String[] distinctVals = new String[distinctAttrs.size()];
		
		HashSet<String> set = new HashSet<String>();
		
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		
		if(isJoin == false)
		{
			int blocks = schema_manager.getRelation(tableNames.get(0)).getNumOfBlocks();
			if(blocks > 9)
			{
				getTableData(tableNames.get(0));
				getTableData(tableNames.get(0));
			}
		}
	
		for(Tuple current : outputTuples)
		{			
			StringBuffer key = new StringBuffer();
			for(int t=0; t<distinctAttrs.size(); t++)
			{
				key.append( current.getField(distinctAttrs.get(t)).toString() + "_" );
			}
			
			if(set.contains(key.toString()))
			{
				//it means its a duplicate tuple
				//do nothing
			}
			else
			{
				result.add(current);
				set.add(new String(key));
			}
		}
		
		outputTuples = result;
		
		return result;
	}
	
	public void joinCleanup(ArrayList<String> tableNames)
	{
		String joinName = tableNames.get(0);
		for(int i=1; i<(tableNames.size()); i++)
		{
			joinName += "join"+tableNames.get(i);
			schema_manager.deleteRelation(joinName);
		}
	}
	
	public ArrayList<Tuple> performJoinaAndOutput(ArrayList<String> tableNames, String whereCondition, ArrayList<String> projectionAttrs)
	{
		//ArrayList<String> projectionAttrs = getProjectionAttributes(true, tableNames);
		ArrayList<Tuple> outputTuples = new ArrayList<Tuple>();
		
		System.out.println(" This is one pass algorithm ");
		String table1Name = tableNames.get(0);
		String table2Name = tableNames.get(1);
		
		int freeMemoryBlocks = 8;
		ArrayList<Relation> relationList = new ArrayList<Relation>();
		for(String table : tableNames)
		{
			relationList.add(schema_manager.getRelation(table));
		}
		
		String smallRName = null;
		String bigRName = null;
		Relation smallR = null;
		Relation bigR = null;
		
		
		
		if((relationList.get(0).getNumOfBlocks() > 8) && (relationList.get(1).getNumOfBlocks() > 8))
		{
			//System.out.println(" This requires a two pass algorithm ");
			performTwoPassJoin(tableNames,whereCondition, new ArrayList<Tuple>(), new ArrayList<Tuple>(), false);
			return new ArrayList<Tuple>();
		}
				
		
		if(relationList.get(0).getNumOfBlocks() < relationList.get(1).getNumOfBlocks())
		{
			smallR = relationList.get(0);
			smallRName = tableNames.get(0);
			bigRName = tableNames.get(1);
			bigR = relationList.get(1);
		}
		else
		{
			smallR = relationList.get(1);
			smallRName = tableNames.get(1);
			bigRName = tableNames.get(0);
			bigR = relationList.get(0);
		}
		//got all blocks in memory
		if( smallR.getNumOfBlocks() < freeMemoryBlocks)
		{
			for(int i=0; i<smallR.getNumOfBlocks(); i++)
			{
				smallR.getBlock(i,i);
			}
		}
		
		Block smallRBlock = mem.getBlock(0);
		Block bigRBlock = null;
		bigR.getBlock(0,8);
		bigRBlock = mem.getBlock(8);
		
		ArrayList<Tuple> smallRTuples = mem.getTuples(0,1);
		ArrayList<Tuple> bigRTuples = mem.getTuples(8, 1);
		
		Tuple t1 = smallRTuples.get(0);
		Tuple t2 = bigRTuples.get(0);
		
		ArrayList<String> fieldNamesSR = t1.getSchema().getFieldNames();
		ArrayList<String> fieldNamesBR = t2.getSchema().getFieldNames();
		ArrayList<String> tempRfieldNames = new ArrayList<String>();
		for( String fName : fieldNamesSR)
		{					
			tempRfieldNames.add(smallRName+"."+fName);
		}
		for( String fName : fieldNamesBR)
		{
			tempRfieldNames.add(bigRName+ "." + fName);
		}
		
		ArrayList<FieldType> tempFieldTypes = t1.getSchema().getFieldTypes();
		ArrayList<FieldType> bigRFieldTypes = t2.getSchema().getFieldTypes();
		
		for( FieldType ftype : bigRFieldTypes)
		{
			tempFieldTypes.add(ftype);
		}
		
		Schema tempSchema = new Schema(tempRfieldNames,tempFieldTypes);
		Relation tempRelation = null;
		
		
		tempRelation = schema_manager.createRelation(table1Name+"join"+table2Name,tempSchema);
			
		
		//now performing join and outputing the table
		for(int i=0; i<smallR.getNumOfBlocks(); i++)
		{
			smallRBlock = mem.getBlock(i);
			bigRBlock = null;
			for( int j = 0; j<bigR.getNumOfBlocks(); j++)
			{
				bigR.getBlock(j,8);
				bigRBlock = mem.getBlock(8);
				
				smallRTuples = mem.getTuples(i,1);
				bigRTuples = mem.getTuples(8, 1);
				
				for(int st=0; st<smallRTuples.size(); st++)
				{
					
					Tuple smallTuple = smallRTuples.get(st);
					
					Tuple tempTuple = tempRelation.createTuple();
					for(String fieldName : fieldNamesSR)
					{
						if (smallTuple.getField(fieldName).type == FieldType.STR20)
						{
							tempTuple.setField(smallRName+"."+fieldName,smallTuple.getField(fieldName).str);
						}
						else
						{
							tempTuple.setField(smallRName+"."+fieldName,smallTuple.getField(fieldName).integer);
						}
					}
					for(int bt = 0; bt<bigRTuples.size(); bt++)
					{
						Tuple bigTuple = bigRTuples.get(bt);
						for( String fieldName : fieldNamesBR)
						{
							if (bigTuple.getField(fieldName).type == FieldType.STR20)
							{
								tempTuple.setField(bigRName+"."+fieldName,bigTuple.getField(fieldName).str);
							}
							else
							{
								tempTuple.setField(bigRName+"."+fieldName,bigTuple.getField(fieldName).integer);
							}
						}
						
						Tuple current = printOneTuple(tempTuple,whereCondition,false);
						if(current != null)
							outputTuples.add(current);
					}
				}
				
			}
		}
		//schema_manager.deleteRelation("temporaryR");	
		return outputTuples;
	}
	
	public ArrayList<String> getProjectionAttributes(boolean isJoin, ArrayList<String> tableNames, ArrayList<String> distinctAttrList)
	{
		ArrayList<String> projectionAttrs = new ArrayList<String>();
		
		String pattern1 = "SELECT";
		String pattern2 = "FROM";
		
		String matchedResult = "";
		
		Pattern p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
		Matcher m = p.matcher(stmt);
		while (m.find()) {
			matchedResult = m.group(1);					  
		}
		
		boolean isPrintAllColumns = false;
		
		if(matchedResult.contains("*"))
		{			
			isPrintAllColumns = true;
		}
		else
		{
			String[] colSplit = matchedResult.split(",");
			
			for(int i=0; i< colSplit.length; i++)
			{
				
				if( colSplit[i].contains("DISTINCT"))
				{	
					distinctAttrList.add(colSplit[i].trim().split(" ")[1].trim());
					projectionAttrs.add(colSplit[i].trim().split(" ")[1].trim());
				}
				else
				{					
					projectionAttrs.add(colSplit[i].trim());
				}
					
			}
		}
		
		if(isPrintAllColumns == true)
		{
			for(int i=0; i<tableNames.size(); i++)
			{
				ArrayList<String> current = schema_manager.getRelation(tableNames.get(i)).getSchema().getFieldNames();
				for(String field : current)
				{
					projectionAttrs.add(tableNames.get(i) + "." + field);
				}
			}
		}
		
		
		
		return projectionAttrs;
	}
	

	public ArrayList<Tuple> performTwoPassJoin(ArrayList<String> tableNames, String whereCondition, ArrayList<Tuple> table1, ArrayList<Tuple> table2, boolean multi)
	{
		//System.out.println(" Inside two pass algorithm ");
		
		ArrayList<Tuple> outputTuples = new ArrayList<Tuple>();
		
		ArrayList<Relation> relationList = new ArrayList<Relation>();
		
		
		String table1Name = tableNames.get(0);
		String table2Name = tableNames.get(1);
		Relation relation1 = null;
		Relation relation2 = null;
		Tuple t1 = null;
		Tuple t2 = null;
		
		ArrayList<Tuple> relation1List = null;
		ArrayList<Tuple> relation2List = null;
		Schema tempSchema =  null;
		
		ArrayList<String> fieldNames1 = null;
		ArrayList<String> fieldNames2 = null;
		ArrayList<String> tempRfieldNames = null;
		
		if(multi == false)
		{
			for(String table : tableNames)
			{
				relationList.add(schema_manager.getRelation(table));
			}
			relation1 = relationList.get(0);
			relation2 = relationList.get(1);		
			relation1.getBlock(0,0);
			relation2.getBlock(0,1);
			t1 = mem.getTuples(0,1).get(0);
			t2 = mem.getTuples(1,1).get(0);
			relation1List = getTableData(table1Name);
			relation2List = getTableData(table2Name);
			 //tempSchema = getJoinSchema(t1.getSchema(), t2.getSchema(), false, table1Name, table2Name);
			fieldNames1 = t1.getSchema().getFieldNames();
			fieldNames2 = t2.getSchema().getFieldNames();
			if(relation1List.size() > 9 && relation2List.size() > 9)
			{
				for(int l =0; l<3; l++)
				{
					getTableData(table1Name);
					getTableData(table2Name);
				}
				
			}
			tempRfieldNames = new ArrayList<String>();
			for( String fName : fieldNames1)
			{					
				tempRfieldNames.add(table1Name+"."+fName);
			}
			for( String fName : fieldNames2)
			{
				tempRfieldNames.add(table2Name+ "." + fName);
			}
		}
		else
		{
			relation1List = table1;
			relation2List = table2;
			t1 = table1.get(0);
			t2 = table2.get(0);
			fieldNames1 = t1.getSchema().getFieldNames();
			fieldNames2 = t2.getSchema().getFieldNames();
			tempRfieldNames = new ArrayList<String>();
			for( String fName : fieldNames1)
			{					
				tempRfieldNames.add(fName);
			}
			for( String fName : fieldNames2)
			{
				tempRfieldNames.add(table2Name+ "." + fName);
			}
			
			
			 //tempSchema = getJoinSchema(t1.getSchema(), t2.getSchema(), true, table1Name, table2Name);
		}
		
		
		
		
		
		ArrayList<FieldType> tempFieldTypes = t1.getSchema().getFieldTypes();
		ArrayList<FieldType> FieldTypes2 = t2.getSchema().getFieldTypes();
		
		for( FieldType ftype : FieldTypes2)
		{
			tempFieldTypes.add(ftype);
		}
		
		tempSchema = new Schema(tempRfieldNames,tempFieldTypes);
		Relation tempRelation = null;
		
		
		tempRelation = schema_manager.createRelation(table1Name+"join"+table2Name,tempSchema);
		//temporary schema created
		
		Tuple tempTuple = null;
		
		tempTuple = tempRelation.createTuple();
		
		for( int i=0; i< relation1List.size(); i++)
		{
			Tuple t1Tuple = relation1List.get(i);
			
				for(String fieldName : fieldNames1)
				{
					if(t1Tuple.getField(fieldName).type == FieldType.INT)
					{
						if(multi == false)
							tempTuple.setField(table1Name+"."+fieldName, t1Tuple.getField(fieldName).integer);
						else
							tempTuple.setField(fieldName, t1Tuple.getField(fieldName).integer);
					}
					else
					{
						if(multi == false)
							tempTuple.setField(table1Name+"."+fieldName, t1Tuple.getField(fieldName).str);
						else
							tempTuple.setField(fieldName, t1Tuple.getField(fieldName).str);
					}
				}
			for(int j=0; j<relation2List.size(); j++)
			{
				Tuple t2Tuple = relation2List.get(j);
				for(String fieldName : fieldNames2)
				{
					if(t2Tuple.getField(fieldName).type == FieldType.INT)
					{
						tempTuple.setField(table2Name+"."+fieldName,t2Tuple.getField(fieldName).integer);
					}
					else
					{
						tempTuple.setField(table2Name+"."+fieldName, t2Tuple.getField(fieldName).str);
					}
				}
				Tuple current = printOneTuple(tempTuple,whereCondition,true);
				if(current != null)
					outputTuples.add(current);
				tempTuple = tempRelation.createTuple();
				//now add 1st values to this 
				for(String fieldName : fieldNames1)
				{
					if(t1Tuple.getField(fieldName).type == FieldType.INT)
					{
						if(multi == false)
							tempTuple.setField(table1Name+"."+fieldName, t1Tuple.getField(fieldName).integer);
						else
							tempTuple.setField(fieldName, t1Tuple.getField(fieldName).integer);
					}
					else
					{
						if(multi == false)
							tempTuple.setField(table1Name+"."+fieldName, t1Tuple.getField(fieldName).str);
						else
							tempTuple.setField(fieldName, t1Tuple.getField(fieldName).str);
					}
				}
			}
		}
		
		
		//schema_manager.deleteRelation("temporaryR");
		return outputTuples;
		
	}
	
	public Schema getJoinSchema(Schema schema1, Schema schema2, boolean multi, String table1Name, String table2Name)
	{
		ArrayList<String> fieldNames1 = schema1.getFieldNames();
		ArrayList<String> fieldNames2 = schema2.getFieldNames();
		ArrayList<String> tempRfieldNames = new ArrayList<String>();
		if( multi == false)
		{
			for( String fName : fieldNames1)
			{					
				tempRfieldNames.add(fName);
			}
		}
		else
		{
			for( String fName : fieldNames1)
			{					
				tempRfieldNames.add(table1Name+"."+fName);
			}
		}
		
		
		for( String fName : fieldNames2)
		{
			tempRfieldNames.add(table2Name+ "." + fName);
		}
		
		ArrayList<FieldType> tempFieldTypes = schema1.getFieldTypes();
		ArrayList<FieldType> FieldTypes2 = schema2.getFieldTypes();
		
		for( FieldType ftype : FieldTypes2)
		{
			tempFieldTypes.add(ftype);
		}
		
		Schema tempSchema = new Schema(tempRfieldNames,tempFieldTypes);
		
		return tempSchema;
		
	}
	
	public void joinTables(Stack<String> tableNames, String whereCondition)
	{
		if(tableNames.size() == 2)
		{
			String table1 = tableNames.pop();
			String table2 = tableNames.pop();
			
			//performJoinMajeMein(table1, table2, );
		}
		else
			joinTables(tableNames, whereCondition);
	}
	
	
	

}