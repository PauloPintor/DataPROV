package com.generic.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

/** 
 * Represents a table in the SQL query to support the parser
 * 
 * @author Paulo Pintor
 * @version 1.0
 * @since 1.0
*/
public class ParserTable {
 	private String name;
    private String database;
    private String schema;
    private String alias;
    private ArrayList<ParserColumn> columns;

	/** 
	* Class constructor.
	*/
    public ParserTable() {
    }

	/**
	* Class constructor specifying the name of the table
	* @param name the name of the table
	*/
    public ParserTable(String name) {
        this.name = name;
    }

	/**
	* Class constructor specifying the name of the table and the alias
	* @param name the name of the table
	* @param alias the alias of the table
	*/
    public ParserTable(String name, String alias) {
        this.name = name;
        this.alias = alias;
    }

	/**
	* Class constructor specifying the name of the table and the array with the columns
	* @param name the name of the table
	* @param columns the ArrayList<ParserColumn> with the columns
	*/
    public ParserTable(String name, ArrayList<ParserColumn> columns) {
        this.name = name;
        this.columns = columns;
    }

	/**
	* Class constructor specifying the name of the table, the alias and the array with the columns
	* @param name the name of the table
	* @param alias the alias of the table
	* @param columns the ArrayList<ParserColumn> with the columns
	*/
    public ParserTable(String name, String alias, ArrayList<ParserColumn> columns) {
        this.name = name;
        this.alias = alias;
        this.columns = columns;       
    }
    
	/** 
	 * Get the name of the table
	 * 
	 * @return String the name of the table
	 */
	public String getName() {
        return this.name;
    }

	/** 
	 * Set the name of the table
	 * 
	 * @param name the name of the table
	 */
    public void setName(String name) {
        this.name = name;
    }

	/**
	 * Get the database of the table
	 * 
	 * @return String the database of the table
	 */
    public String getDatabase() {
        return database;
    }

	/**
	 * Set the database of the table
	 * 
	 * @param database the database of the table
	 */
    public void setDatabase(String database) {
        this.database = database;
    }

	/**
	 * Get the schema of the table
	 * 
	 * @return String the schema of the table
	 */
    public String getSchema() {
        return this.schema;
    }

	/**
	 * Set the schema of the table
	 * 
	 * @param schema the schema of the table
	 */
    public void setSchema(String schema) {
        this.schema = schema;
    }

	/**
	 * Get the alias of the table
	 * 
	 * @return String the alias of the table
	 */
    public String getAlias() {
        return this.alias;
    }

	/**
	 * Set the alias of the table
	 * 
	 * @param alias the alias of the table
	 */
    public void setAlias(String alias) {
        this.alias = alias;
    }

	/**
	 * Get the columns of the table
	 * 
	 * @return ArrayList<ParserColumn> containing the columns of the table
	 */
    public ArrayList<ParserColumn> getColumns() {
        return this.columns;
    }

	/**
	 * Set the columns of the table
	 * 
	 * @param columns an ArrayList<ParserColumn> containing the columns of the table
	 */
    public void setColumns(ArrayList<ParserColumn> columns) {
        this.columns = columns;
    }

	/**
	 * Add a column to the table
	 * 
	 * @param column the column to be added
	 */
    public void addColumn(ParserColumn column)
    {
        if(columns == null)
            columns = new ArrayList<>();
        
        columns.add(column);        
    }

	//TODO ver se ainda é necessário
    public void addColumns(List<SelectItem> selectedItems){
        int index = 0;
        for (SelectItem selectItem : selectedItems) {
            if (selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                net.sf.jsqlparser.schema.Column column = (net.sf.jsqlparser.schema.Column) selectExpressionItem.getExpression();
                String columnName = column.getColumnName();
                String columnAlias = selectExpressionItem.getAlias() != null ? selectExpressionItem.getAlias().getName() : null;

                if(column.getTable() != null)
                {
                    if(alias != null && alias.compareTo(column.getTable().getName()) == 0 || 
                        name.compareTo(column.getTable().getName()) == 0)
                        this.addColumn(new ParserColumn(columnName, columnAlias, index));
                }
                index++;
            }            
        }
    }

	/**
	 * Returns a String with the name of ..... //TODO complete this
	 * 
	 * @return String with the columns of the table
	 */
    public String toStringColumns(){
        String columns = "";
        for(ParserColumn c : this.columns)
            columns += (c.getAlias() != null ? c.getAlias() : c.getName()) + ",";
        columns = columns.replaceAll(",$", "");
        return "["+columns+"]";
    }

    @Override
    public String toString(){
        System.out.println(" ");
        return (this.database != null ? this.database + "." : "") + (this.schema != null ? this.schema + "." : "") +
        this.name;
    }

	/**
	 * Returns a String with the database name, the schema and the table separated by '.' (database.schema.table) 
	 * and the alias if it exists
	 * 
	 * @return String with following format (database.schema.table AS alias)
	 */
    public String toStringWithAlias(){
        return (this.database != null ? this.database + "." : "") + (this.schema != null ? this.schema + "." : "") +
        this.name + (this.alias != null ? " AS " + this.alias : "");
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ParserTable)) {
            return false;
        }
        ParserTable table = (ParserTable) o;
        return Objects.equals(name, table.name) && Objects.equals(database, table.database) && Objects.equals(schema, table.schema) && Objects.equals(alias, table.alias);
    }
}
