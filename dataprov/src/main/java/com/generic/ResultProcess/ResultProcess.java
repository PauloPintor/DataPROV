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

import org.matheclipse.core.eval.ExprEvaluator;
import org.matheclipse.core.expression.F;
import org.matheclipse.core.interfaces.IExpr;

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
				
				if(rsmd.getColumnName(i).toLowerCase().equals("prov"))
				{
					String prov = _result.getString(rsmd.getColumnName(i)).replaceAll("\u2297","x").replaceAll("\u2295", "+");
					row.put("how", prov);
					row.put("why", processWhy(prov));
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
	}

	public List<LinkedHashMap<String, Object>> getResult() {
		return result;
	}

	public void printResult(){
		LinkedHashMap<String, Integer> columnWidths = getColumnWidths(result);

		for (String column : columnWidths.keySet()) {
			System.out.format("%-" + columnWidths.get(column) + "s | ", column);
		}
		System.out.println();

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
		String why = "";
		Pattern pattern = Pattern.compile("\\+\\s*\\d+(\\.\\d+([Ee][+-]?\\d+)?)?\\s*");
        Matcher matcher = pattern.matcher(prov);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, "");
        }
        matcher.appendTail(sb);
       
		// Regular expression pattern to match words containing ':'
        String regex = "\\w+:[\\w:]+";
        
        pattern = Pattern.compile(regex);
        
        matcher = pattern.matcher(sb.toString());
        
		int i = 0;
		
		HashMap<String, String> mapTokens = new HashMap<String, String>();

		String mathResult = "";
		prov = sb.toString();

		if(prov.indexOf(") .") != -1 || prov.indexOf(". (") != -1)
		{

			while (matcher.find()) {
				String word = matcher.group();
				if(!mapTokens.containsKey(word)){
					mapTokens.put(word, "x"+i);
					prov = prov.replace(word, "x"+i);
					i++;
				}
			} 

			ExprEvaluator util = new ExprEvaluator();
			IExpr expr = util.eval(prov);

			IExpr result = util.eval(F.Distribute(expr));
			
			String regexMath = "(\\w+)\\^\\d+";
			mathResult = result.toString().replace(regexMath, "$1");

			why = "{" + mathResult.replace(".", ",")
						    .replace("+", "},{")
							+ "}";
			
			for (Map.Entry<String, String> entry : mapTokens.entrySet()) 
				why = why.replaceAll(entry.getValue(),entry.getKey());	
		}
		else
		{

			why = prov.replace(" . ", ",")
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
}