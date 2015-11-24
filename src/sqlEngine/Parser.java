package sqlEngine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storageManager.*;

public class Parser {
	
	public static void main(String[] args)
	{
		//ReadFile rf = new ReadFile("C:/Users/RatneshThakur/Desktop/Course Materials/Database Systems/Database Systems Project 2/TinySQL_windows - Copy.txt"); //contains file name which contains sql statement
		
		ReadFile rf = new ReadFile("C:/Users/RatneshThakur/Desktop/Course Materials/Database Systems/Database Systems Project 2/TinySQL_windows.txt"); //contains file name which contains sql statement
		
		ArrayList<String> fileData = rf.readFileText();	//contains file data in the form of array list
		
		System.out.println(" Size of the file : " + fileData.size() + " lines");
		Statement st = null;
		
		
		 //=======================Initialization=========================
	    System.out.print("=======================Initialization=========================" + "\n");

	    // Initialize the memory, disk and the schema manager
	    MainMemory mem=new MainMemory();
	    Disk disk=new Disk();
	    //System.out.print("The memory contains " + mem.getMemorySize() + " blocks" + "\n");
	    //System.out.print(mem + "\n" + "\n");
	    SchemaManager schema_manager=new SchemaManager(mem,disk);

	    disk.resetDiskIOs();
	    disk.resetDiskTimer();

		
		for(int i=0; i<fileData.size(); i++)
		{			
			st = new Statement(fileData.get(i), mem,disk, schema_manager);
//			try{
//				st.analyzeStatement();
//			}
//			catch(Exception ex)
//			{
//				System.out.println(" Some error occured -- We are looking into it");
//			}			
			st.analyzeStatement();
		}		
	}
	
	public static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) {
	    Block block_reference;
	    if (relation_reference.getNumOfBlocks()==0) {
	      //System.out.print("The relation is empty" + "\n");
	      //System.out.print("Get the handle to the memory block " + memory_block_index + " and clear it" + "\n");
	      block_reference=mem.getBlock(memory_block_index);
	      block_reference.clear(); //clear the block
	      block_reference.appendTuple(tuple); // append the tuple
	      //System.out.print("Write to the first block of the relation" + "\n");
	      relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
	    } else {
	      //System.out.print("Read the last block of the relation into memory block 5:" + "\n");
	      relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
	      block_reference=mem.getBlock(memory_block_index);

	      if (block_reference.isFull()) {
	        //System.out.print("(The block is full: Clear the memory block and append the tuple)" + "\n");
	        block_reference.clear(); //clear the block
	        block_reference.appendTuple(tuple); // append the tuple
	        //System.out.print("Write to a new block at the end of the relation" + "\n");
	        relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
	      } else {
	        //System.out.print("(The block is not full: Append it directly)" + "\n");
	        block_reference.appendTuple(tuple); // append the tuple
	        //System.out.print("Write to the last block of the relation" + "\n");
	        relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
	      }
	    }
	  }

}
