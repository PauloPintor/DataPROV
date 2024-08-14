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
import java.nio.file.Paths;
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
	boolean booleanResult = false;
	boolean trioResult = false;
	boolean posResult = false;
	boolean lineageResult = false;

	public ResultProcess(boolean why, boolean booleanResult, boolean trioResult, boolean posResult, boolean lineageResult) {
		this.why = why;
		this.booleanResult = booleanResult;
		this.trioResult = trioResult;
		this.posResult = posResult;
		this.lineageResult = lineageResult;
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
					if(why || booleanResult || trioResult || posResult || lineageResult) 
						processProvenance(prov, row);
					//if (why) row.put("why", processWhy(prov));
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

	public void processProvenance(String prov, LinkedHashMap<String, Object> row) throws IOException {
		String whyProv = "";
		String booleanProv = "";
		String trioProv = "";
		String posProv = "";
		String lineageProv = "";


		prov = prov.replaceAll("\\.(min|avg|sum|max|count)\\s+\\d+(\\.\\d+)?", "");
		prov = prov.replaceAll("\\. (\\d+(\\.\\d+)?)", "");

		// Regular expression pattern to match words containing ':'
		String regex = "\\w+:[\\w:]+";
        
        Pattern pattern = Pattern.compile(regex);
        
        //Matcher matcher = pattern.matcher(sb.toString());
		Matcher matcher = pattern.matcher(prov);
        
		int i = 0;
		
		HashMap<String, String> mapTokens = new HashMap<String, String>();

		prov = prov.replaceAll("\u2297","*").replaceAll("\u2295", "+");

		int j = 0;

		while (matcher.find()) {
			j++;
			String word = matcher.group();
			if(!mapTokens.containsKey(word)){
				mapTokens.put(word, "x"+i);
				prov = prov.replace(word, "x"+i);
				i++;
			}
		} 

		if(j == 0)
		{
			regex = "\\b[a-zA-Z_][a-zA-Z0-9_]*\\b";
			pattern = Pattern.compile(regex);
			matcher = pattern.matcher(prov);
			while (matcher.find()) {
				j++;
				String word = matcher.group();
				if(!mapTokens.containsKey(word)){
					mapTokens.put(word, "x"+i);
					prov = prov.replace(word, "x"+i);
					i++;
				}
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
			throw new IOException("Error creating temporary file", e);
		}
		
		
		callMathSolver(tempFile.getAbsolutePath());

		List<String> lines = Files.readAllLines(Paths.get(tempFile.getAbsolutePath()));
		for(i = 0; i < lines.size(); i++){
			if(i == 0 && lines.get(i) != "" && booleanResult == true){
				booleanProv = lines.get(i);
				for (Map.Entry<String, String> entry : mapTokens.entrySet())
					booleanProv = booleanProv.replaceAll(entry.getKey(),entry.getValue());
				row.put("boolean", booleanProv);
			}else if(i == 1 && lines.get(i) != "" && trioResult == true){
				trioProv = lines.get(i);
				for (Map.Entry<String, String> entry : mapTokens.entrySet())
					trioProv = trioProv.replaceAll(entry.getKey(),entry.getValue());
				row.put("trio", trioProv);
			}else if(i == 2 && lines.get(i) != "" && why == true){
				whyProv = lines.get(i);
				for (Map.Entry<String, String> entry : mapTokens.entrySet())
				whyProv = whyProv.replaceAll(entry.getKey(),entry.getValue());
				row.put("why", posProv);
			}else if(i == 3 && lines.get(i) != "" && posResult == true){
				posProv = lines.get(i);
				for (Map.Entry<String, String> entry : mapTokens.entrySet())
					posProv = posProv.replaceAll(entry.getKey(),entry.getValue());
				row.put("pos", posProv);
			}
		}
		
		if(lineageResult == true){
			StringBuilder keysConcatenated = new StringBuilder();
			for (String key : mapTokens.keySet()) {
				if (keysConcatenated.length() > 0) {
					keysConcatenated.append(", ");
				}
				keysConcatenated.append(key);
			}

			lineageProv = "{"+ keysConcatenated.toString() +"}";
			row.put("lineage", lineageProv);
		}

		tempFile.delete();
	}

    private void callMathSolver(String path) throws IOException{
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
		List<String> commands = new ArrayList<>();
		commands.add("/Users/paulopintor/.local/pipx/venvs/sympy/bin/Python3");
		commands.add(tempFile.getAbsolutePath());
		commands.add("-path");
		commands.add(path);
		commands.add("-wp");
		commands.add(String.valueOf(why));
		commands.add("-wb");
		commands.add(String.valueOf(booleanResult));
		commands.add("-wt");
		commands.add(String.valueOf(trioResult));
		commands.add("-wpos");
		commands.add(String.valueOf(posResult));
		
        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        Process process = processBuilder.start();

        // Capture the output
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append(System.lineSeparator());
            }
        }
		System.out.println(output.toString());

		// Capture the error output from the script
		StringBuilder error = new StringBuilder();
		BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
		String errorLine;
		while ((errorLine = errorReader.readLine()) != null) {
			error.append(errorLine).append(System.lineSeparator());
		}

        try {
            int exitCode = process.waitFor();
            if(exitCode != 0)
                throw new IOException(error.toString());
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while waiting for the Python script to finish", e);
        }
	}
}
