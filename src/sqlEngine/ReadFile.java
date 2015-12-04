package sqlEngine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class ReadFile
{
	String fileName;
	ArrayList<String> fileData;
	public ReadFile(String var_filename)
	{
		fileName = var_filename;
	}
	
	public ArrayList<String> readFileText()
	{
		BufferedReader in = null;
		try{
			in = new BufferedReader(
					new FileReader(fileName));
			fileData = new ArrayList<String>();
			String str;
			while ((str = in.readLine()) != null)
			{
				fileData.add(str);				
			}
			in.close();			
		}
		catch(Exception ex)
		{
			System.out.println("File not found");			
		}
		
		return fileData;
		
	}
}