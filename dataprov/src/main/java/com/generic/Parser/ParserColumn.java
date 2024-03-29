package com.generic.Parser;

import java.util.Objects;

/** 
 * Represents a column in the SQL query to support the parser
 * 
 * @author Paulo Pintor
 * @version 1.0
 * @since 1.0
*/
public class ParserColumn{
	private String name;
    private String alias;
    private String asName;
    private int order;
    private ParserTable table;
	private String unionId;

	/**
     * The class constructor
     * @param name the column's name
     * @param alias the column's alias if it has one
     * @param order the column's order in the projection
     */
    public ParserColumn(String name, String alias, int order){
        this.name = name;
        this.alias = alias;
        this.order = order;
    }

    /** 
     * Get the name of the column
     * 
     * @return String the column's name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Set the name of the column
     * 
     * @param name the column's name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the alias of the column
     * 
     * @return String the column's alias
     */
    public String getAlias() {
        return this.alias;
    }

    /**
     * Set the alias of the column
     * 
     * @param alias the column's alias
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }

    /**
     * Get the asName of the column
     * 
     * @return String the column's asName
     */
    public String getAsName() {
        return this.asName;
    }

    /**
     * Set the asName of the column
     * 
     * @param asName the column's asName
     */
    public void setAsName(String asName) {
        this.asName = asName;
    }

    /**
     * Get the order of the column
     * 
     * @return int the column's order in the projection
     */
    public int getOrder() {
        return this.order;
    }

    /**
     * Set the order of the column
     * 
     * @param order the column's order in the projection
     */
    public void setOrder(int order) {
        this.order = order;
    }

    /**
     * Get the table of the column
     * 
     * @return ParserTable the column's table
     */
    public ParserTable getTable() {
        return table;
    }

    /**
     * Set the table of the column
     * 
     * @param table the column's table
     */
    public void setTable(ParserTable table) {
        this.table = table;
    }

	/**
	 * Get the unionId of the column
	 * 
	 * @return the column's unionId
	 */
	public String getUnionId() {
		return unionId;
	}

	/**
	 * Set the unionId of the column
	 * @param unionId the unionId of the column
	 */
	public void setUnionId(String unionId) {
		this.unionId = unionId;
	}

	public String toString() {
		return table.toString()+":"+name;
	}

	@Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof ParserColumn)) {
            return false;
        }
        ParserColumn column = (ParserColumn) o;
        //return Objects.equals(name, column.name) && Objects.equals(alias, column.alias) && order == column.order && Objects.equals(table, column.table);
		return Objects.equals(name, column.name) && Objects.equals(alias, column.alias) && Objects.equals(table, column.table);
    }

	@Override
    public int hashCode() {    
        return (this.name.hashCode() + this.table.hashCode());        
    }
}
