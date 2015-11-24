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
		
		ArrayList<String> attrList = new ArrayList<String>();
		
		ArrayList<String> distinctAttrList = new ArrayList<String>();
		boolean distinctPresent = false;
		
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
					colSplit[i] = colSplit[i].substring(colSplit[i].lastIndexOf(".") + 1);
					System.out.println(" distinct column found ");
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
		
		whereCondition = getTableNames(tableNames,pattern1,pattern2);
				
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
			for( int i=0; i<fieldNames.size(); i++)
			{
				System.out.print(fieldNames.get(i));
				System.out.print("   |  ");
			}
			System.out.println(" ");
			System.out.println("----------------------------------------------------");
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
						if(isPartOfQuery == false && distinctPresent == false)
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
					if( isPartOfQuery == false && distinctPresent == false)
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
			printDistinctTuples(outputTuplesList, distinctAttrList, fieldNames);
			//printDistinctRows(output, distinctAttrList);
		}
			
		
		return output;
	}
	
	private void printDistinctTuples( ArrayList<Tuple> outputTuples, ArrayList<String> distinctAttrs, ArrayList<String> fieldNames)
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
		
		for(int i=0; i<result.size(); i++)
		{
			for( int j=0; j<fieldNames.size(); j++)
			{
				System.out.print("\t" + result.get(i).getField(fieldNames.get(j)) + "");
			}
			System.out.println("  ");
		}
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
	
	public boolean twoPassSort(Relation relation_reference)
	{
		int mainMemorySize = (int)Math.sqrt((double)relation_reference.getNumOfBlocks());
		for(int i=0; i<relation_reference.getNumOfBlocks(); i++)
		{
			for(int j=0; j<mainMemorySize; j++)
			{
				relation_reference.getBlock(i+j, 2+j);
			}
			System.out.println("Memory right now " + mem);
		}
		
		return true;
	}
	
	public boolean onePassSort(Relation relation_reference)
	{
		for(int i=0; i< relation_reference.getNumOfBlocks(); i++)
		{
			relation_reference.getBlock(i,2+i);
		}
		
		System.out.println("Memory state during one pass sort " + mem);
		return true;
	}
	
	
}