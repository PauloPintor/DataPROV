package com.Parser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

public class NestedTables {
	private String table;
	private Table joinTable;
	private Expression joinExp;

	public NestedTables(String table, Table joinTable, Expression joinExp) {
		this.table = table;
		this.joinTable = joinTable;
		this.joinExp = joinExp;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public Table getJoinTable() {
		return joinTable;
	}

	public void setJoinTable(Table joinTable) {
		this.joinTable = joinTable;
	}

	public Expression getJoinExp() {
		return joinExp;
	}

	public void setJoinExp(Expression joinExp) {
		this.joinExp = joinExp;
	}
}
