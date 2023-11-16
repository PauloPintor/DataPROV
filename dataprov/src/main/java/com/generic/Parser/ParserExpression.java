package com.generic.Parser;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ParserExpression {
	private SubSelect select;
	private Table joinTable;
	private List<Expression> joinExp;
	private List<Expression> whereExpressions;

	public ParserExpression(){}

	public void setSelect(SubSelect select){
		this.select = select;
	}

	public SubSelect getSelect(){
		return this.select;
	}

	public Table getJoinTable() {
		return joinTable;
	}

	public void setJoinTable(Table joinTable) {
		this.joinTable = joinTable;
	}

	public List<Expression> getJoinExpression() {
		return joinExp;
	}

	public void addJoinExpression(Expression expression){
		if(joinExp == null)
			this.joinExp = new ArrayList<Expression>();
		this.joinExp.add(expression);
	}

	public void setJoinExpressions(List<Expression> expressions){
		this.joinExp = expressions;
	}

	public void setJoinExpressions(Expression expression){
		if(joinExp == null)
			this.joinExp = new ArrayList<Expression>();
		this.joinExp.add(expression);
	}

	public int getJoinExpSize(){
		return this.joinExp.size();
	} 

	public List<Expression> getWhereExpressions() {
		return whereExpressions;
	}

	public void setWhereExpressions(List<Expression> whereExpressions) {
		this.whereExpressions = whereExpressions;
	}

	public void addWhereExpression(Expression expression){
		if(whereExpressions == null)
			this.whereExpressions = new ArrayList<Expression>();
		this.whereExpressions.add(expression);
	}


	public boolean HasWhereExp(){
		return this.whereExpressions != null && this.whereExpressions.size() > 0;
	}

}


