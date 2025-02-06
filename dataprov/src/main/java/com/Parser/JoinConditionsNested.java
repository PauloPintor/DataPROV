package com.Parser;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class JoinConditionsNested {
	private Expression whereExpression = null;
	private Expression expJoins = null;
	private List<Column> columns;
	private List<NestedTables> joins;
	private int count = 0;
	private List<Table> joinTables;
	private String newTableName;
	private boolean exists;

	public JoinConditionsNested(List<Table> joinTables, String newTableName, boolean exists) {
		this.joinTables = joinTables;
		this.columns = new ArrayList<>();
		this.joins = new ArrayList<>();
		this.newTableName = newTableName;
		this.exists = exists;
	}

	public void splitConditionsWithOperators(Expression expression,String currentOperator)
	{
		if (expression instanceof AndExpression) {
			AndExpression andExpression = (AndExpression) expression;
	
			// Recursively process left and right expressions with the "AND" operator
			splitConditionsWithOperators(andExpression.getLeftExpression(), currentOperator.equals("OR") ? "OR" : "AND");
			splitConditionsWithOperators(andExpression.getRightExpression(), "AND");
	
		} else if (expression instanceof OrExpression) {
			OrExpression orExpression = (OrExpression) expression;
	
			// Recursively process left and right expressions with the "OR" operator
			splitConditionsWithOperators(orExpression.getLeftExpression(), "OR");
			splitConditionsWithOperators(orExpression.getRightExpression(), "OR");
	
		} else if (expression instanceof BinaryExpression) {
			BinaryExpression binaryExpression = (BinaryExpression) expression;

			if (binaryExpression.getLeftExpression() instanceof Column 
				&& binaryExpression.getRightExpression() instanceof Column) {
				Column leftColumn = (Column) binaryExpression.getLeftExpression();
				Column rightColumn = (Column) binaryExpression.getRightExpression();

				// Pass to joinCondition with the appropriate operator
				joinCondition(leftColumn, rightColumn, binaryExpression, currentOperator);
			}else{
				if(whereExpression == null) {
					whereExpression = binaryExpression;
				} else {
					if(currentOperator == "OR") {
						whereExpression = new OrExpression(whereExpression, binaryExpression);
					} else {
						whereExpression = new AndExpression(whereExpression, binaryExpression);
					}
				}
			}
		}else{
			if(whereExpression == null) {
				whereExpression = expression;
			} else {
				if(currentOperator == "OR") {
					whereExpression = new OrExpression(whereExpression, expression);
				} else {
					whereExpression = new AndExpression(whereExpression, expression);
				}
			}
		}
	}

	private void joinCondition(Column leftColumn, Column rightColumn, BinaryExpression binaryExpression, String currentOperator) {
		String leftTable = leftColumn.getTable().getName();
		String rightTable = rightColumn.getTable().getName();
		if(!leftTable.equals(rightTable)) {
			if(isTableInJoinTables(rightTable) && isTableInJoinTables(leftTable)) {
				if(whereExpression == null) {
					whereExpression = binaryExpression;
				} else {
					if(currentOperator == "OR") {
						whereExpression = new OrExpression(whereExpression, binaryExpression);
					} else {
						whereExpression = new AndExpression(whereExpression, binaryExpression);
					}
				}
			}else if(!isTableInJoinTables(rightTable) && isTableInJoinTables(leftTable)) {
				
				if(exists){
					if(getIndexOfTable(rightTable) == -1){
						
						Table nested = new Table();
						nested.setName(rightTable);
						nested.setAlias(new Alias((exists ? "nestedT"+count : newTableName)));
						Column column = new Column();
						String columnName = rightColumn.getColumnName();
						column.setColumnName(columnName);
						column.setTable(nested);
						Column columnprov = new Column();
						columnprov.setColumnName("prov");
						columnprov.setTable(nested);
						
						columns.add(column);
						columns.add(columnprov);
						joins.add(new NestedTables(rightTable, nested, combineExpression(binaryExpression, (Column) binaryExpression.getLeftExpression(), column)));	
						count++;
					}else{
						Expression joinExp = joins.get(getIndexOfTable(rightTable)).getJoinExp();
						Column column = new Column();
						String columnName = rightColumn.getColumnName();
						column.setColumnName(columnName);
						column.setTable(joins.get(getIndexOfTable(rightTable)).getJoinTable());
						columns.add(column);

						if(currentOperator == "OR") {
							joins.get(getIndexOfTable(rightTable)).setJoinExp(new OrExpression(joinExp, combineExpression(binaryExpression, (Column) binaryExpression.getLeftExpression(), column)));
						} else {
							joins.get(getIndexOfTable(rightTable)).setJoinExp(new AndExpression(joinExp, combineExpression(binaryExpression, (Column) binaryExpression.getLeftExpression(), column)));
						}
					}

				
					Column column = new Column();
					String columnName = rightColumn.getColumnName();
					column.setColumnName(columnName);
					column.setTable(new Table(null, newTableName));
					
					if(expJoins == null){
						expJoins = equiJoins(column, (Column)binaryExpression.getRightExpression());
					}else if(currentOperator == "OR"){
						expJoins = new OrExpression(expJoins,equiJoins(column, (Column)binaryExpression.getRightExpression()));
					}else{
						expJoins = new AndExpression(expJoins,equiJoins(column, (Column)binaryExpression.getRightExpression()));
					}
				}else{
					Column column = new Column();
					String columnName = leftColumn.getColumnName();
					column.setColumnName(columnName);
					column.setTable(new Table(null, newTableName));
					if(!colmunsContainCol(leftColumn)) columns.add(leftColumn);
					/*Column column2 = new Column();
					columnName = rightColumn.getColumnName();
					column2.setColumnName(columnName);
					column2.setTable(new Table(null, newTableName));
					if(!colmunsContainCol(column)) columns.add(column);
					if(!colmunsContainCol(column2)) columns.add(column2);*/
					
					if(expJoins == null){
						expJoins = combineExpression(binaryExpression, column, rightColumn);
					}else if(currentOperator == "OR"){
						expJoins = new OrExpression(expJoins,combineExpression(binaryExpression, column, rightColumn));
					}else{
						expJoins = new AndExpression(expJoins,combineExpression(binaryExpression, column, rightColumn));
					}
				}
				
			}else if(isTableInJoinTables(rightTable) && !isTableInJoinTables(leftTable)) {
				if(exists){
					if(getIndexOfTable(leftTable) == -1){
						Table nested = new Table();
						nested.setName(leftTable);
						nested.setAlias(new Alias((exists ? "nestedT"+count : newTableName)));
						Column column = new Column();
						String columnName = leftColumn.getColumnName();
						column.setColumnName(columnName);
						column.setTable(nested);
						Column columnprov = new Column();
						columnprov.setColumnName("prov");
						columnprov.setTable(nested);
						
						columns.add(column);
						columns.add(columnprov);
							
						joins.add(new NestedTables(leftTable, nested, combineExpression(binaryExpression, column, (Column) binaryExpression.getRightExpression())));	

						count++;
					}else{
						Expression joinExp = joins.get(getIndexOfTable(leftTable)).getJoinExp();
						Column column = new Column();
						String columnName = leftColumn.getColumnName();
						column.setColumnName(columnName);
						column.setTable(joins.get(getIndexOfTable(leftTable)).getJoinTable());
						columns.add(column);
						if(currentOperator == "OR") {
							joins.get(getIndexOfTable(leftTable)).setJoinExp(new OrExpression(joinExp, combineExpression(binaryExpression, column, (Column) binaryExpression.getRightExpression())));
						} else {
							joins.get(getIndexOfTable(leftTable)).setJoinExp(new AndExpression(joinExp, combineExpression(binaryExpression, column, (Column) binaryExpression.getRightExpression())));
						}
					}

				
					Column column = new Column();
					String columnName = leftColumn.getColumnName();
					column.setColumnName(columnName);
					column.setTable(new Table(null, newTableName));
					
					if(expJoins == null){
						expJoins = equiJoins((Column)binaryExpression.getLeftExpression(), column);
					}else if(currentOperator == "OR"){
						expJoins = new OrExpression(expJoins,equiJoins((Column)binaryExpression.getLeftExpression(), column));
					}else{
						expJoins = new AndExpression(expJoins,equiJoins((Column)binaryExpression.getLeftExpression(), column));
					}
				}else{


					Column column = new Column();
					String columnName = rightColumn.getColumnName();
					column.setColumnName(columnName);
					column.setTable(new Table(null, newTableName));
					if(!colmunsContainCol(rightColumn)) columns.add(rightColumn);
					/*Column column2 = new Column();
					columnName = rightColumn.getColumnName();
					column2.setColumnName(columnName);
					column2.setTable(new Table(null, newTableName));
					if(!colmunsContainCol(column)) columns.add(column);
					if(!colmunsContainCol(column2)) columns.add(column2);*/
					if(expJoins == null){
						expJoins = combineExpression(binaryExpression, leftColumn, column);
					}else if(currentOperator == "OR"){
						expJoins = new OrExpression(expJoins,combineExpression(binaryExpression, leftColumn, column));
					}else{
						expJoins = new AndExpression(expJoins,combineExpression(binaryExpression, leftColumn, column));
					}
				}
			}
		}else{
			if(whereExpression == null) {
				whereExpression = binaryExpression;
			} else {
				if(currentOperator == "OR") {
					whereExpression = new OrExpression(whereExpression, binaryExpression);
				} else {
					whereExpression = new AndExpression(whereExpression, binaryExpression);
				}
			}
		}
	}

	private BinaryExpression combineExpression(BinaryExpression expression, Column left, Column right) {
		if(expression instanceof EqualsTo) {
			return new EqualsTo(left, right);
		}else if(expression instanceof GreaterThan) {
			return new GreaterThan(left, right);
		}else if(expression instanceof GreaterThanEquals) {
			return new GreaterThanEquals(left, right);
		}else if(expression instanceof MinorThan) {
			return new MinorThan(left, right);
		}else if(expression instanceof MinorThanEquals) {
			return new MinorThanEquals(left, right);
		}else if(expression instanceof NotEqualsTo) {
			return new NotEqualsTo(left, right);
		}
		return null;
	}

	private boolean colmunsContainCol(Column column){
		for(Column col : columns){
			if(col.getColumnName().equals(column.getColumnName()) && col.getTable().getName().equals(column.getTable().getName()))
				return true;
		}
		return false;

	}

	private BinaryExpression equiJoins(Column left, Column right) {
		return new EqualsTo(left, right);
	}

	private boolean isTableInJoinTables(String tableName) {
		for (Table table : joinTables) {
			if ((table.getAlias() != null && table.getAlias().getName().equals(tableName)) || table.getName().equals(tableName)) {
				return true;
			}
		}
		return false;
	}

	public Expression getWhereExpression() {
		return whereExpression;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public List<NestedTables> getJoins() {
		return joins;
	}

	public Expression getExpJoins() {
		return expJoins;
	}

	private int getIndexOfTable(String table){
		for(int i = 0; i < joins.size(); i++){
			if(joins.get(i).getTable().equals(table))
				return i;
		}
		return -1;
	}
}
