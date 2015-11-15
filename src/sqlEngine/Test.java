package sqlEngine;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;




public class Test {
	public static void main(String[] args)
	{
		//String[] matches = match("aa11bb22", "/(\\d+)/g" ); // => ["11", "22"]
		 List<String> allMatches = new ArrayList<String>();
		 Matcher m = Pattern.compile("\\{([^}]*)\\}")
		     .matcher("some string with {the data i want} inside {and a lot of other things}");
		 while (m.find()) {
		   allMatches.add(m.group());
		   
		 }
		 System.out.println("here " + allMatches.size());
		 for(int i=0; i<allMatches.size(); i++)
		 {
			 System.out.println(allMatches.get(i));
		 }
		
	}
}
