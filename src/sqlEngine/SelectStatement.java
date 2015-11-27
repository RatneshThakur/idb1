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
		ArrayList<String> orderByList = new ArrayList<String>();
		
		ArrayList<String> attrList = new ArrayList<String>();
		
		ArrayList<String> distinctAttrList = new ArrayList<String>();
		boolean distinctPresent = false;
		boolean isOrderByPresent = false;
		boolean isPerformJoin = false;
		
		if(stmt.contains("ORDER BY"))
			isOrderByPresent = true;
		
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
		
		
		//getting all table names now
		pattern1 = "FROM";
		pattern2 = "WHERE";
		
		// we get 
		//1. tablesNames
		//2. Order by list of attributes
		
		whereCondition = getTableNames(tableNames,pattern1,pattern2, orderByList);
		
		if(tableNames.size() > 1)
		{			
			isPerformJoin = true;
			performJoinaAndOutput(tableNames,whereCondition);
			//performTwoPassJoin(tableNames, whereCondition, isOrderByPresent);
			return new ArrayList<ArrayList<String>>();
		}
				
		//parsing logic ends here
		
		//following code is assumed for only one table name in from.. Later need to expand it to join 
		relation_reference = schema_manager.getRelation(tableNames.get(0));
		
		//test twoPasssort algorithms
		//twoPassSort(relation_reference);
		//onePassSort(relation_reference);
		
		//was getting everything in once
		//relation_reference.getBlocks(0,3,relation_reference.getNumOfBlocks());
	    
		
		ArrayList<String> fieldNames = new ArrayList<String>();
		if(isPrintAllColumns == true)
		 fieldNames = relation_reference.getSchema().getFieldNames();
		else
		  fieldNames = attrList;
		
		if( isPartOfQuery == false)
		{
			System.out.print("\t");
			for( int i=0; i<fieldNames.size(); i++)
			{
				System.out.print(fieldNames.get(i));
				System.out.print("   |  ");
			}
			System.out.println(" ");
			System.out.println("-----------------------------------------------------------------------");
		}
		
		
		ArrayList<ArrayList<String>> output = new ArrayList<ArrayList<String>>();
		
		ArrayList<String> aListOfValues = new ArrayList<String>();
		
		ArrayList<Tuple> outputTuplesList = new ArrayList<Tuple>();
		
		for(int i=0; i<relation_reference.getNumOfBlocks(); i++)
		{			
			relation_reference.getBlock(i,3);
			
			Block block_reference=mem.getBlock(3);
			
			//System.out.println(" No of tuples in this block " + block_reference.getNumTuples());
			
			//System.out.println("block dump is " + block_reference);
			int numOfTuples = block_reference.getNumTuples();
			for(int k=0; k<numOfTuples; k++)
			{

				Tuple current = block_reference.getTuple(k);				
				if(current.isNull() == true)
					numOfTuples++;				
				else if(testCondition(current,whereCondition) == true)
				{
					outputTuplesList.add(current);
					for(int j=0; j<fieldNames.size(); j++)
					{
						if(isPartOfQuery == false && distinctPresent == false && isOrderByPresent == false)
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
					if( isPartOfQuery == false && distinctPresent == false && isOrderByPresent == false)
						System.out.println(" ");
					else
					{
						output.add(aListOfValues);
						aListOfValues = new ArrayList<String>();
					}
				}
				
			}
			
			
		}
		
		if(aListOfValues.size() > 0)
		{
			output.add(aListOfValues);
		}
		
		if(distinctPresent == true)
		{
			//System.out.println(" distinct attrs " + distinctAttrList);
			//System.out.println(" fieldNames are " + fieldNames);
			if(isPrintAllColumns == true )
				distinctAttrList = fieldNames;
			outputTuplesList = printDistinctTuples(outputTuplesList, distinctAttrList, fieldNames);
			//printDistinctRows(output, distinctAttrList);
		}
		if(isOrderByPresent == true)
		{
			sortTuplesByColumn(outputTuplesList,orderByList);
		}
		
		if( isOrderByPresent == true || distinctPresent == true)
			printTuples(outputTuplesList, fieldNames);
		
		return output;
	}
	
	
	
	private void sortTuplesByColumn(ArrayList<Tuple> outputTuples, ArrayList<String> orderByList)
	{
		String field = orderByList.get(0);
		field = field.substring(field.lastIndexOf(".") + 1);
		Collections.sort(outputTuples, new MyComparator(field));
	}
	
	private ArrayList<Tuple> printDistinctTuples( ArrayList<Tuple> outputTuples, ArrayList<String> distinctAttrs, ArrayList<String> fieldNames)
	{
		String[] distinctVals = new String[distinctAttrs.size()];
		
		HashSet<String> set = new HashSet<String>();
		
		ArrayList<Tuple> result = new ArrayList<Tuple>();
	
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
//		for(int i=0; i<result.size(); i++)
//		{
//			for( int j=0; j<fieldNames.size(); j++)
//			{
//				System.out.print("\t" + result.get(i).getField(fieldNames.get(j)) + "");
//			}
//			System.out.println("  ");
//		}
		
		return result;
	}
	
	private boolean printDistinctRows(ArrayList<ArrayList<String>> output, ArrayList<String> distinctAttr)
	{
		for(int i=0; i<output.size(); i++)
		{
			ArrayList<String> current = output.get(i);
			String[] distinctVal = new String[distinctAttr.size()];
			for( int t =0; t < distinctVal.length; t++)
			{
				distinctVal[t] = current.get(t);
			}
			
			for( int j = 0; j<output.size(); j++)
			{
				if( j == i)
					continue;
				//System.out.println( "   tuple is  " + );
				boolean isDiff = false;
				for( int tj = 0; tj<distinctVal.length; tj++)
				{
					if(!distinctVal[tj].equals(output.get(j).get(tj)))
						isDiff = true;
				}
				if(isDiff == false)
				{
					output.remove(j);
				}
			}
		}
		
		for( int i=0; i<output.size(); i++)
		{
			ArrayList<String> current = output.get(i);
			for( int j =0; j<current.size(); j++)
			{
				System.out.print(" \t " + current.get(j));
			}
			System.out.println(" ");
		}
		return true;
	}
	
	public void performJoinaAndOutput(ArrayList<String> tableNames, String whereCondition)
	{
		ArrayList<String> projectionAttrs = getProjectionAttributes(true, tableNames);		
		
		System.out.println(" This is one pass algorithm ");
		
		
		
		System.out.println("---------------------------------------------------------------------------------------------------------");
		for(int i=0; i<projectionAttrs.size(); i++)
		{
			System.out.print("\t"+projectionAttrs.get(i)+"|");
		}
		System.out.println(" ");
		
//		if(true)
//		{
//			//System.out.println(" performing two pass algorithm ");
//			performTwoPassJoin(tableNames,whereCondition,false);
//			return;
//		}
			
		
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
		
		System.out.println(" No of blocks are " + relationList.get(0).getNumOfBlocks() + " " +relationList.get(1).getNumOfBlocks());
		
		if((relationList.get(0).getNumOfBlocks() > 8) && (relationList.get(1).getNumOfBlocks() > 8))
		{
			System.out.println(" This requires a two pass algorithm ");
			performTwoPassJoin(tableNames,whereCondition,false);
			return;
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
		
		tempRelation = schema_manager.createRelation("temporaryR",tempSchema);
			
		
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
					
						printOneTuple(tempTuple,whereCondition, projectionAttrs);
					}
				}
				
			}
		}
		schema_manager.deleteRelation("temporaryR");	
	}
	
	public ArrayList<String> getProjectionAttributes(boolean isJoin, ArrayList<String> tableNames)
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
			System.out.println("printing of all columns is required");
			isPrintAllColumns = true;
		}
		else
		{
			String[] colSplit = matchedResult.split(",");
			
			for(int i=0; i< colSplit.length; i++)
			{
				
				if( colSplit[i].contains("DISTINCT"))
				{	
					
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
				System.out.println(" ");
				for(String field : current)
				{
					projectionAttrs.add(tableNames.get(i) + "." + field);
				}
			}
		}
		
		return projectionAttrs;
	}
	

	public void performTwoPassJoin(ArrayList<String> tableNames, String whereCondition, boolean isOrderByPresent)
	{
		System.out.println(" Inside two pass algorithm ");
		
		ArrayList<String> projectionAttrs = getProjectionAttributes(true,tableNames);
		
		System.out.println(" Will be project these columns ");
		
		System.out.println("---------------------------------------------------------------------------------------------------------");
		for(int i=0; i<projectionAttrs.size(); i++)
		{
			System.out.print("\t"+projectionAttrs.get(i)+"|");
		}
		System.out.println(" ");
				

		int freeMemoryBlocks = 8;
		ArrayList<Relation> relationList = new ArrayList<Relation>();
		for(String table : tableNames)
		{
			relationList.add(schema_manager.getRelation(table));
		}
		
		String table1Name = tableNames.get(0);
		String table2Name = tableNames.get(1);
		
		Relation relation1 = relationList.get(0);
		Relation relation2 = relationList.get(1);
		
		relation1.getBlock(0,0);
		relation2.getBlock(0,1);
		//fieldNames1 for relation 1 
		//fieldNames2 for relation 2
		Tuple t1 = mem.getTuples(0,1).get(0);
		Tuple t2 = mem.getTuples(1,1).get(0);
		
		ArrayList<String> fieldNames1 = t1.getSchema().getFieldNames();
		ArrayList<String> fieldNames2 = t2.getSchema().getFieldNames();
		ArrayList<String> tempRfieldNames = new ArrayList<String>();
		for( String fName : fieldNames1)
		{					
			tempRfieldNames.add(table1Name+"."+fName);
		}
		for( String fName : fieldNames2)
		{
			tempRfieldNames.add(table2Name+ "." + fName);
		}
		
		ArrayList<FieldType> tempFieldTypes = t1.getSchema().getFieldTypes();
		ArrayList<FieldType> FieldTypes2 = t2.getSchema().getFieldTypes();
		
		for( FieldType ftype : FieldTypes2)
		{
			tempFieldTypes.add(ftype);
		}
		
		Schema tempSchema = new Schema(tempRfieldNames,tempFieldTypes);
		Relation tempRelation = null;
		
		tempRelation = schema_manager.createRelation("temporaryR",tempSchema);
		//temporary schema created
		ArrayList<Tuple> relation1List = new ArrayList<Tuple>();
		for(int i=0; i<relation1.getNumOfBlocks(); i++)
		{
			mem.getBlock(0).clear();
			relation1.getBlock(i,0);			
			ArrayList<Tuple> t1List = mem.getTuples(0,1);
			for(int t=0; t<t1List.size(); t++)
			{
				relation1List.add(t1List.get(t));
			}
		}
		ArrayList<Tuple> relation2List = new ArrayList<Tuple>();
		for( int i=0; i<relation2.getNumOfBlocks(); i++)
		{
			mem.getBlock(8).clear();
			relation2.getBlock(i,8);
			ArrayList<Tuple> t2List = mem.getTuples(8,1);
			for(int t=0; t<t2List.size(); t++)
			{
				relation2List.add(t2List.get(t));
			}
			
		}
		
		System.out.println(" Number of tuples in 1 and 2 are " + relation1List.size() + " and  " + relation2List.size());
		Tuple tempTuple = null;
		
		for( int i=0; i< relation1List.size(); i++)
		{
			Tuple t1Tuple = relation1List.get(i);
			tempTuple = tempRelation.createTuple();
				for(String fieldName : fieldNames1)
				{
					if(t1Tuple.getField(fieldName).type == FieldType.INT)
					{
						tempTuple.setField(table1Name+"."+fieldName, t1Tuple.getField(fieldName).integer);
					}
					else
					{
						tempTuple.setField(table1Name+"."+fieldName, t1Tuple.getField(fieldName).str);
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
				printOneTuple(tempTuple,whereCondition, projectionAttrs);
				
			}
		}
		
		
		schema_manager.deleteRelation("temporaryR");
		
	}
	
	

	
}