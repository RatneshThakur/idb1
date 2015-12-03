package sqlEngine;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storageManager.*;

public class Parser {
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException
	{
		//ReadFile rf = new ReadFile("C:/Users/RatneshThakur/Desktop/Course Materials/Database Systems/Database Systems Project 2/TinySQL_windows - Copy.txt"); //contains file name which contains sql statement
		
		ReadFile rf = new ReadFile("C:/Users/RatneshThakur/Desktop/Course Materials/Database Systems/Database Systems Project 2/TinySQL_windows.txt"); //contains file name which contains sql statement
		
		ArrayList<String> fileData = rf.readFileText();	//contains file data in the form of array list
		
		//System.out.println(" Size of the file : " + fileData.size() + " lines");
		Statement st = null;
		
		
		 //=======================Initialization=========================
	    System.out.print("=======================Initialization=========================" + "\n\n");

	    // Initialize the memory, disk and the schema manager
	    MainMemory mem=new MainMemory();
	    Disk disk=new Disk();
	    PrintWriter writer = new PrintWriter("Output.txt", "UTF-8");
	    //System.out.print("The memory contains " + mem.getMemorySize() + " blocks" + "\n");
	    //System.out.print(mem + "\n" + "\n");
	    SchemaManager schema_manager=new SchemaManager(mem,disk);

	   

		
		for(int i=0; i<fileData.size(); i++)
		{			
			st = new Statement(fileData.get(i), mem,disk, schema_manager,writer);
//			try{
//				st.analyzeStatement();
//			}
//			catch(Exception ex)
//			{
//				System.out.println(" Some error occured -- We are looking into it");
//			}
			
			disk.resetDiskIOs();
			disk.resetDiskTimer();
			
			st.analyzeStatement();
			
			writer.println("Disk I/O: " + disk.getDiskIOs());
			writer.println("Execution time: " + disk.getDiskTimer());
			writer.println(" ");
			
			System.out.println("Disk I/O: " + disk.getDiskIOs());
			System.out.println("Execution time: " + disk.getDiskTimer());
			System.out.println(" ");
		}		
	}
	
	public static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) {
	    Block block_reference;
	    if (relation_reference.getNumOfBlocks()==0) {	      
	      block_reference=mem.getBlock(memory_block_index);
	      block_reference.clear(); //clear the block
	      block_reference.appendTuple(tuple); // append the tuple
	      relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
	    } else {
	      relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
	      block_reference=mem.getBlock(memory_block_index);
	      if (block_reference.isFull()) {
	        block_reference.clear(); //clear the block
	        block_reference.appendTuple(tuple); // append the tuple
	        relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
	      } else {
	        block_reference.appendTuple(tuple); // append the tuple
	        relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
	      }
	    }
	  }

}
