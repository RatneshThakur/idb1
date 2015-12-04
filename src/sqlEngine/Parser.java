package sqlEngine;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storageManager.*;

public class Parser {
	
	public static void main(String[] args) throws IOException
	{
		//ReadFile rf = new ReadFile("C:/Users/RatneshThakur/Desktop/Course Materials/Database Systems/Database Systems Project 2/TinySQL_windows - Copy.txt"); //contains file name which contains sql statement
		
		ReadFile rf = new ReadFile("C:/Users/RatneshThakur/Desktop/Course Materials/Database Systems/Database Systems Project 2/TinySQL_windows.txt"); //contains file name which contains sql statement
		
		ArrayList<String> fileData = rf.readFileText();	//contains file data in the form of array list
		
		//System.out.println(" Size of the file : " + fileData.size() + " lines");
		Statement st = null;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String line = null;
		boolean fromFile = false;
		System.out.println("Do you want to input queries or read from file? Enter 'input' for first and 'file' for second ");
		System.out.println("Type exit to exit the program at any point");
		
		 //=======================Initialization=========================
	    System.out.print("=======================Initialization=========================" + "\n\n");

	    // Initialize the memory, disk and the schema manager
	    MainMemory mem=new MainMemory();
	    Disk disk=new Disk();
	    PrintWriter writer = new PrintWriter("Output.txt");
	    SchemaManager schema_manager=new SchemaManager(mem,disk);

		
		while(true)
		{
			line = br.readLine();
			if(line.equals("exit"))
				return;
			else if(line.equals("file"))
			{
				fromFile = true;
				break;
			}
			else if(line.equals("input"))
			{
				fromFile = false;
				break;
			}
			else
			{
				System.out.println("Invalid input. Run program again");
				return;
			}
		}
		
		if(fromFile == true)
		{
			System.out.println("Provide file location");
			String fileLocation = br.readLine();
			rf = new ReadFile(fileLocation); //contains file name which contains sql statement
			
			fileData = rf.readFileText();
			if(fileData == null)
			{				
				return;
			}
			if(fileData.size() == 0)
				return;
			for(int i=0; i<fileData.size(); i++)
			{			
				st = new Statement(fileData.get(i), mem,disk, schema_manager,writer);
				
				disk.resetDiskIOs();
				disk.resetDiskTimer();
				
				try{
					st.analyzeStatement();
				}
				catch(Exception ex)
				{
					System.out.println("Some error occured while running this query");
				}
				
				
				writer.println("Disk I/O: " + disk.getDiskIOs());
				writer.println("Execution time: " + disk.getDiskTimer());
				writer.println(" ");
				
				System.out.println("Disk I/O: " + disk.getDiskIOs());
				System.out.println("Execution time: " + disk.getDiskTimer());
				System.out.println(" ");
			}
			writer.close();
			
		}
		else
		{
			//reading lines one by one
			while(true)
			{
				line = br.readLine();
				if(line.equals("exit"))
				{
					System.out.println("Program ends");
					return;
				}
				st = new Statement(line, mem,disk, schema_manager,writer);
				disk.resetDiskIOs();
				disk.resetDiskTimer();
				
				try{
					st.analyzeStatement();
				}
				catch(Exception ex)
				{
					System.out.println("Some error occured while running this query");
				}
				
				
				writer.println("Disk I/O: " + disk.getDiskIOs());
				writer.println("Execution time: " + disk.getDiskTimer());
				writer.println(" ");
				
				System.out.println("Disk I/O: " + disk.getDiskIOs());
				System.out.println("Execution time: " + disk.getDiskTimer());
				System.out.println(" ");
				
				
			}
			
			
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
