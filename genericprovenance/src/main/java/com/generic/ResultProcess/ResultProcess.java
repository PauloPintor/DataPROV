package com.generic.ResultProcess;

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
import java.util.stream.Collectors;

import org.matheclipse.core.eval.ExprEvaluator;
import org.matheclipse.core.expression.F;
import org.matheclipse.core.interfaces.IExpr;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class ResultProcess {
	List<LinkedHashMap<String, Object>> result = new ArrayList<LinkedHashMap<String, Object>>();

	public ResultProcess() {
	}

	public void processing(ResultSet _result) throws SQLException {
		ResultSetMetaData rsmd = _result.getMetaData();
		int columnsNumber = rsmd.getColumnCount();

		while (_result.next()) {
			LinkedHashMap<String, Object> row = new LinkedHashMap<>();

			for (int i = 1; i <= columnsNumber; i++) {
				
				//System.out.print(rsmd.getColumnName(i) + " (" + rsmd.getColumnTypeName(i) + ") ");
				if(rsmd.getColumnName(i).toLowerCase().equals("prov"))
				{
					System.out.println(_result.getString(rsmd.getColumnName(i)));
					String prov = _result.getString(rsmd.getColumnName(i)).replaceAll("x","\u2297");
					row.put("how", prov);
					row.put("why", processWhy(prov));
				}	
				else
				{
					//TODO deal with the problem of the same column name
					Object columnValue = _result.getObject(rsmd.getColumnName(i));
					System.out.println(columnValue);
					row.put(rsmd.getColumnName(i), columnValue);
				}
			}

			result.add(row);
		}

	}

	public List<LinkedHashMap<String, Object>> getResult() {
		return result;
	}

	public void printResult(){
		// Get the column headers and their maximum widths
		LinkedHashMap<String, Integer> columnWidths = getColumnWidths(result);

		// Print the table headers
		for (String column : columnWidths.keySet()) {
			System.out.format("%-" + columnWidths.get(column) + "s | ", column);
		}
		System.out.println();

		// Print the table data
		for (LinkedHashMap<String, Object> row : result) {
			for (String column : columnWidths.keySet()) {
				Object value = row.get(column);
				System.out.format("%-" + columnWidths.get(column) + "s ", value != null ? value : "");
			}
			System.out.println();
		}
	}

	// Helper method to get column headers and their maximum widths
    private LinkedHashMap<String, Integer> getColumnWidths(List<LinkedHashMap<String, Object>> data) {
        LinkedHashMap<String, Integer> columnWidths = new LinkedHashMap<>();
        for (LinkedHashMap<String, Object> row : data) {
            for (String column : row.keySet()) {
                int width = column.length();
                if (columnWidths.containsKey(column)) {
                    width = Math.max(width, columnWidths.get(column));
                }
                columnWidths.put(column, width);
            }
        }
        return columnWidths;
    }

	private String processWhy(String prov) {
		
		Pattern pattern = Pattern.compile("\u2297\\s*\\d+(\\.\\d+([Ee][+-]?\\d+)?)?\\s*");
        Matcher matcher = pattern.matcher(prov);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, "");
        }
        matcher.appendTail(sb);
        prov = sb.toString();
		
		// Regular expression pattern to match words containing ':'
        String regex = "\\w+:[\\w:]+";
        
        // Create a Pattern object
        pattern = Pattern.compile(regex);
        
        // Create a Matcher object
        matcher = pattern.matcher(prov);
        
		int i = 0;
		BiMap<String, String> mapTokens = HashBiMap.create();
		//HashMap<String, String> map = new HashMap<String, String>();

        // Find and print all matching words
        while (matcher.find()) {
            String word = matcher.group();
			mapTokens.put(word, "x"+i);
			i++;
        } 

		for (Map.Entry<String, String> entry : mapTokens.entrySet()) {
			//System.out.println(entry.getKey());
			//System.out.println(entry.getValue());
			prov = prov.replaceAll(entry.getKey(), entry.getValue());	
		}

		ExprEvaluator util = new ExprEvaluator();
		IExpr expr = util.eval(prov);
		//IExpr expr = util.eval("((x4 * x3) + (x3 * x4))");

		IExpr result = util.eval(F.Distribute(expr));
		// print: a*b+a*c
		String regexMath = "(\\w+)\\^\\d+";
		String mathResult = result.toString();
		mathResult = mathResult.replaceAll(regexMath, "$1");

		//System.out.println(mathResult);

		String[] list = mathResult.split("\\+");
		String why = "";
		for (String string : list) {
			why = why + "{";
			for(String s : string.split("\\.")){
				String _key = mapTokens.inverse().get(s);
				if(_key != null){
					why = why + _key + ",";
				}
			}
			why = why.substring(0,why.length()-1)+ "},";
		}

		return "{"+why.substring(0,why.length()-1)+"}";
	}
}