package com.generic.Helpers;

import java.io.InputStream;
import java.util.Properties;

public class ConfigHelper {
	/**
	 * configPath contains the name of the configuration file
	 */
	private String configPath = "config.properties";
	
	/**
	 * The class constructor
	 */
	public ConfigHelper() {};
	
	/**
	 * The function returns the value of a property defined in the config file.
	 * The file should be created under the dir src/main/resources or change the
	 * private string configPath in this file.
	 * 
	 * @param propertyName
	 * @return the value of the property with the param name
	 * @throws Exception
	 */
	public String getPropertieValue(String propertyName) throws Exception {
		InputStream input = ConfigHelper.class.getClassLoader().getResourceAsStream(configPath);
        Properties prop = new Properties();

        if (input == null)
            throw new Exception("File config.properties is not created or the path is wrong.");

        //load a properties file from class path, inside static method
        prop.load(input);

        //get the property value and print it out
        return prop.getProperty(propertyName);
	}
}
