package sqlEngine;

import java.util.*;
public class InfixToPostFix {
	public static void main(String[] args)
	{
		String where = "homework = 100 AND project = 98";
		Stack<String> s = new Stack<String>();
		ArrayList<String> postFix = createPostFix(where,s);
		
		//System.out.println("Result of expression is " + evaluatePostFix(postFix));
		
		
		while(s.size() > 0)
		{
			String current = s.pop();
			System.out.println(current);
		}
	}
	
	private static ArrayList<String> createPostFix(String where, Stack<String> s)
	{
		String[] tokens = where.split(" ");
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
		
		for(String lit: postFix)
			System.out.print(" " + lit + " ");
	
		return postFix;
	}
	
	private static String evaluatePostFix(ArrayList<String> postFix)
	{
		if(postFix.size() == 0)
			return "0";
		HashMap<String, Integer> opMap = new HashMap<String,Integer>();
		opMap.put("+", 1);
		opMap.put("/", 2);
		opMap.put("*", 2);
		opMap.put("-",1);
		opMap.put("=",1);
		opMap.put("AND",1);
		opMap.put("OR",1);
		opMap.put(">", 1);
		
		Stack<String> output = new Stack<String>();
		
		for(int i=0; i<postFix.size(); i++)
		{
			if(opMap.containsKey(postFix.get(i)))
			{
				String operand1 = output.pop();
				String operand2 = output.pop();
				int result=0;
				if(postFix.get(i).equals("+"))
					result = Integer.parseInt(operand1)+ Integer.parseInt(operand2);
				
				output.push(new Integer(result).toString());				
			}
			else
				output.push(postFix.get(i));
		}
		
		return output.pop();
	}

}
