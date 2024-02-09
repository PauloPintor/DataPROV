package com.generic.ResultProcess;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.matheclipse.core.eval.ExprEvaluator;
import org.matheclipse.core.expression.F;
import org.matheclipse.core.interfaces.IExpr;

public class ResultProcess {
	List<LinkedHashMap<String, Object>> result = new ArrayList<LinkedHashMap<String, Object>>();
	boolean why = false;
	public ResultProcess(boolean why) {
		this.why = why;
	}

	public void processing(ResultSet _result) throws SQLException {
		ResultSetMetaData rsmd = _result.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		double howlength = 0;
		while (_result.next()) {
			LinkedHashMap<String, Object> row = new LinkedHashMap<>();
			for (int i = 1; i <= columnsNumber; i++) {
				
				if(rsmd.getColumnName(i).toLowerCase().equals("prov"))
				{
					//String prov = _result.getString(rsmd.getColumnName(i)).replaceAll("\u2297","x").replaceAll("\u2295", "+");
					String prov = _result.getString(rsmd.getColumnName(i)).replaceAll("\\b(\\w+)\\s*:\\(", "(");
					//howlength += prov.length();
					row.put("how", prov);
					if (why) row.put("why", processWhy(prov));
				}	
				else
				{
					//TODO deal with the problem of the same column name
					Object columnValue = _result.getObject(rsmd.getColumnName(i));
					row.put(rsmd.getColumnName(i), columnValue);
				}
			}

			result.add(row);
			
		}
		//System.out.println("howlength: " + howlength);
		//System.out.println("result.size(): " + result.size());
		//System.out.println(howlength/result.size());
	}

	public List<LinkedHashMap<String, Object>> getResult() {
		return result;
	}

	public void printResult(){
		LinkedHashMap<String, Integer> columnWidths = getColumnWidths(result);
		System.out.println();
		System.out.println("**************************RESULTS**************************");
		System.out.println();
		int totalColumns = columnWidths.keySet().size();
		for (String column : columnWidths.keySet()) {
			if(totalColumns == 1)
				System.out.format("%-" + (columnWidths.get(column) - 7) + "s", column);
			else
				System.out.format("%-" + columnWidths.get(column) + "s | ", column);
			totalColumns--;
		}
		System.out.println();

		for (LinkedHashMap<String, Object> row : result) {
			for (String column : columnWidths.keySet()) {
				Object value = row.get(column);
				System.out.format("%-" + columnWidths.get(column) + "s | ", value != null ? value : "");
			}
			System.out.println();
		}
	}

	// Helper method to get column headers and their maximum widths
    private LinkedHashMap<String, Integer> getColumnWidths(List<LinkedHashMap<String, Object>> data) {
        LinkedHashMap<String, Integer> columnWidths = new LinkedHashMap<>();
        for (LinkedHashMap<String, Object> row : data) {
            for (String column : row.keySet()) {
				Object value = row.get(column);
                int width = column.length() > value.toString().length() ? column.length() : value.toString().length();
                if (columnWidths.containsKey(column)) {
                    width = Math.max(width, columnWidths.get(column));
                }
				
				columnWidths.put(column, width);
            }
        }
        return columnWidths;
    }

	private String processWhy(String prov) {
		String why = "";

		//Pattern pattern = Pattern.compile("\\+\\s*\\d+(\\.\\d+([Ee][+-]?\\d+)?)?\\s*");
		//Pattern pattern = Pattern.compile("\\.(?:sum|avg|ming|max)?\\s+\\d+(\\.\\d+)?\\s*");
		//Pattern pattern = Pattern.compile("\\.(?:sum|avg|min|max)?sum\\s+\\d+(\\.\\d+)?\\s*");
		/*Pattern pattern = Pattern.compile("\\.(min|avg|sum|max|count)\\s+\\d+(\\.\\d+)?");
		Matcher matcher = pattern.matcher(prov);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			matcher.appendReplacement(sb, "");
		}
		matcher.appendTail(sb);
		*/
		prov = prov.replaceAll("\\b(\\w+)\\s*:\\(", "(");
		prov = prov.replaceAll("\\.(min|avg|sum|max|count)\\s+\\d+(\\.\\d+)?", "");
		prov = prov.replaceAll("\\. (\\d+(\\.\\d+)?)", "");
		// Regular expression pattern to match words containing ':'
		String regex = "\\w+:[\\w:]+";
        
        Pattern pattern = Pattern.compile(regex);
        
        //Matcher matcher = pattern.matcher(sb.toString());
		Matcher matcher = pattern.matcher(prov);
        
		int i = 0;
		
		HashMap<String, String> mapTokens = new HashMap<String, String>();

		String mathResult = "";
		//prov = sb.toString();
		prov = prov.replaceAll("\u2297","*").replaceAll("\u2295", "+");
		//prov = prov.replaceAll("\\s+(?=\\))", "");
		if(prov.indexOf(") *") != -1 || prov.indexOf("* (") != -1)
		{
			//System.out.println("Parser used!");
			while (matcher.find()) {
				String word = matcher.group();
				if(!mapTokens.containsKey(word)){
					mapTokens.put(word, "x"+i);
					prov = prov.replace(word, "x"+i);
					i++;
				}
			} 
			
			//ExprEvaluator util = new ExprEvaluator();
			//IExpr expr = util.eval(prov);
			//IExpr result = util.eval(F.Expand(expr));
			try {
				PrintWriter out = new PrintWriter("prov.txt");
				out.println(prov);
				out.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			why = callMathSolver();

			/*String regexMath = "(\\w+)\\^\\d+";
			mathResult = result.replaceAll(regexMath, "$1");

			why = "{" + mathResult.replace("*", ",")
						    .replace("+", "},{")
							+ "}";
			why = why.replaceAll("\\{\\d+,", "{");
			*/
			for (Map.Entry<String, String> entry : mapTokens.entrySet()) 
				why = why.replaceAll(entry.getValue(),entry.getKey());
		}
		else
		{

			why = prov.replace(" * ", ",")
							 .replace("+", ",")
							 .replace("(", "{")
							 .replace(")", "}");

			if(why.contains("{{") || why.contains("}}")){
				why = why.replace("{{", "{")
					     .replace("}}", "}");
			}

		}		

		return "{"+why+"}";
	}

	private String callMathSolver(){
		String line = "";
		try{       
			// Create ProcessBuilder
			//ProcessBuilder pb = new ProcessBuilder("python3", "src/main/resources/mathSolver.py");
			ProcessBuilder pb = new ProcessBuilder("python3", "mathSolver.py");
			
			// Start the process
			Process process = pb.start();
			
			// Get the input stream
			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			
			// Read the output
			/* 
            while ((line = reader.readLine()) != null) {
                System.out.println("Python Output: " + line);
            }*/
			line = reader.readLine();
			//System.out.println("Python Output: " + line);
			
			// Wait for the process to finish
			int exitCode = process.waitFor();
			if(exitCode != 0)
				System.out.println("Python script exited with code " + exitCode);
			//System.out.println("Python script exited with code " + exitCode);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

		return line;

	}
}