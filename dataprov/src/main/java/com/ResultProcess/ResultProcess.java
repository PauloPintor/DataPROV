package com.ResultProcess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
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

public class ResultProcess {
    List<LinkedHashMap<String, Object>> result = new ArrayList<LinkedHashMap<String, Object>>();
	boolean why = false;
	public ResultProcess(boolean why) {
		this.why = why;
	}

	public void processing(ResultSet _result) throws SQLException, IOException {
		ResultSetMetaData rsmd = _result.getMetaData();
		int columnsNumber = rsmd.getColumnCount();

		while (_result.next()) {
			LinkedHashMap<String, Object> row = new LinkedHashMap<>();
			for (int i = 1; i <= columnsNumber; i++) {
				
				if(rsmd.getColumnName(i).toLowerCase().equals("prov"))
				{
					String prov = _result.getString(rsmd.getColumnName(i)).replaceAll("\\b(\\w+)\\s*:\\(", "(");
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

	private String processWhy(String prov) throws IOException {
		String why = "";


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
			
			File tempFile = null;
			try {
				tempFile = File.createTempFile("prov", ".txt");

				// Escrever o conteúdo no arquivo
				FileWriter fileWriter = new FileWriter(tempFile);
				BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
				bufferedWriter.write(prov);
				bufferedWriter.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			why = callMathSolver(tempFile.getAbsolutePath());

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

    private String callMathSolver(String path) throws IOException{
		 // Load the Python script from the resources
        InputStream inputStream = ResultProcess.class.getResourceAsStream("/mathSolver.py");
        if (inputStream == null) {
            throw new IOException("Python script not found in resources");
        }

        // Create a temporary file for the Python script
        Path tempScript = Files.createTempFile("script", ".py");
        File tempFile = tempScript.toFile();
        tempFile.deleteOnExit();  // Ensure the file is deleted on exit

        // Write the script content to the temporary file
        try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }

        // Run the Python script using the `python` command
        ProcessBuilder processBuilder = new ProcessBuilder("python3", tempFile.getAbsolutePath(), path);
        Process process = processBuilder.start();

        // Capture the output
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append(System.lineSeparator());
            }
        }

        try {
            int exitCode = process.waitFor();
            if(exitCode != 0)
                throw new IOException(output.toString());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return output.toString();
	}
}
