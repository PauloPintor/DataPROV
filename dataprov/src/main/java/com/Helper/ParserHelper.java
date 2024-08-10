package com.Helper;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
//import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
//import net.sf.jsqlparser.statement.select.SubSelect;

public class ParserHelper {
	public Expression buildAndExpression(List<Expression> expressions) {
        int size = expressions.size();

        if (size == 0) {
            return null;
        } else if (size == 1) {
            return expressions.get(0);
        } else if (size == 2) {
            return new AndExpression(expressions.get(0), expressions.get(1));
        } else {
            int middle = size / 2;
            List<Expression> leftExpressions = expressions.subList(0, middle);
            List<Expression> rightExpressions = expressions.subList(middle, size);

            Expression leftAndExpression = buildAndExpression(leftExpressions);
            Expression rightAndExpression = buildAndExpression(rightExpressions);

            return new AndExpression(leftAndExpression, rightAndExpression);
        }
    }

	public List<Expression> changeToEquals(List<Expression> expressions, Table table, String alias){
		List<Expression> newExpressions = new ArrayList<Expression>();
		for(Expression expression : expressions){
			EqualsTo _equalsTo = null;
			if(!(expression instanceof EqualsTo)){
				if(expression instanceof GreaterThan){
					GreaterThan greaterThan = (GreaterThan) expression;
					_equalsTo = new EqualsTo(greaterThan.getRightExpression(), greaterThan.getLeftExpression());
				}else if(expression instanceof MinorThan){
					MinorThan minorThan = (MinorThan) expression;
					_equalsTo = new EqualsTo(minorThan.getRightExpression(), minorThan.getLeftExpression());
				}else if(expression instanceof GreaterThanEquals){
					GreaterThanEquals greaterThanEquals = (GreaterThanEquals) expression;
					_equalsTo = new EqualsTo(greaterThanEquals.getRightExpression(), greaterThanEquals.getLeftExpression());
				}else if(expression instanceof MinorThanEquals){
					MinorThanEquals minorThanEquals = (MinorThanEquals) expression;
					_equalsTo = new EqualsTo(minorThanEquals.getRightExpression(), minorThanEquals.getLeftExpression());
				}else if(expression instanceof NotEqualsTo){
					NotEqualsTo notEqualsTo = (NotEqualsTo) expression;
					_equalsTo = new EqualsTo(notEqualsTo.getRightExpression(), notEqualsTo.getLeftExpression());
				}
			}else{
				_equalsTo = (EqualsTo) expression;
			}

			ParserHelper parserHelper = new ParserHelper();
			Column column = (Column) _equalsTo.getLeftExpression();
			if(!parserHelper.areTablesEqual(column.getTable(),table)){
				newExpressions.add(new EqualsTo(_equalsTo.getRightExpression(), new Column().withColumnName(column.getColumnName()).withTable(new Table(alias))));
				//column = new Column().withColumnName(column.getColumnName()).withTable(column.getTable());

			}else{
				 column = (Column) _equalsTo.getRightExpression();
				 //column = new Column().withColumnName(column.getColumnName()).withTable(column.getTable());
				 newExpressions.add(new EqualsTo(new Column().withColumnName(column.getColumnName()).withTable(new Table(alias)), _equalsTo.getLeftExpression()));
				
			}
			
		}
		return newExpressions;		
	}

	public boolean areTblEqualsTblSelect(Table table1, Object table2) {

		if(table2 instanceof Table)
			return areTablesEqual(table1, (Table) table2);
		else if(table2 instanceof ParenthesedSelect){
			ParenthesedSelect subSelect = (ParenthesedSelect) table2;
			if(subSelect.getAlias() != null)
				return table1.getName().equals(subSelect.getAlias().getName());
			else
				return false;
		}

		return false;
	}

	public boolean areTablesEqual(Table table1, Table table2) {
		// Compare the table names
		if(table1 == null || table2 == null)
			return false;
			
		String tableName1 = table1.getName();
		String tableName2 = table2.getName();

		// Compare the table aliases (if any)
		String alias1 = (table1.getAlias() != null) ? table1.getAlias().getName() : null;
		String alias2 = (table2.getAlias() != null) ? table2.getAlias().getName() : null;

		// Check if the names and aliases match
		return tableName1.equals(tableName2) && ((alias1 == null && alias2 == null) || alias1.equals(alias2)) || tableName1.equals(alias2) || tableName2.equals(alias1);
	}

	public boolean areColumnsEquals(SelectItem<?> col1, SelectItem<?> col2) {
		// Compare the table names
		String colName1 = col1.getExpression() instanceof Column ? ((Column) col1.getExpression()).getFullyQualifiedName() : "";
		String colName2 = col2.getExpression() instanceof Column ? ((Column) col2.getExpression()).getFullyQualifiedName() : "";

		String tableName1 = col1.getExpression() instanceof Column ? (((Column) col1.getExpression()).getTable() != null ? ((Column) col1.getExpression()).getTable().getName() : "") : "";
        String tableName2 = col2.getExpression() instanceof Column ? (((Column) col2.getExpression()).getTable() != null ? ((Column) col1.getExpression()).getTable().getName() : "") : "";
        
		// Compare the table aliases (if any)
		String alias1 = col1.getAlias() != null ? col1.getAlias().getName() : null;
		String alias2 = col2.getAlias() != null ? col2.getAlias().getName() : null;

		// Check if the names and aliases match
		return colName1.equals(colName2) && ((alias1 == null && alias2 == null) || alias1.equals(alias2)) || tableName1.equals(alias2) || tableName2.equals(alias1);
	}

	public List<Table> extractJoinTables(PlainSelect plainSelect ) {
        List<Table> joinTables = new ArrayList<>();

		// First, add the table from the main "FROM" clause
		FromItem mainFromItem = plainSelect.getFromItem();
		if (mainFromItem instanceof Table) {
			joinTables.add((Table) mainFromItem);
		}else if(mainFromItem instanceof ParenthesedSelect){
			ParenthesedSelect subSelect = (ParenthesedSelect) mainFromItem;
			joinTables.add(new Table(subSelect.getAlias().getName()));
		}

		// Then, iterate over the join clauses and add join tables
		if (plainSelect.getJoins() != null) {
			for (Join join : plainSelect.getJoins()) {
				FromItem joinFromItem = join.getRightItem();
				if (joinFromItem instanceof Table) {
					joinTables.add((Table) joinFromItem);
				}else if(joinFromItem instanceof ParenthesedSelect){
					ParenthesedSelect subSelect = (ParenthesedSelect) joinFromItem;
					joinTables.add(new Table(subSelect.getAlias().getName()));
				}
				
			}
		}
    
        return joinTables;
    }

	public ExpressionList<Expression> equiJoins(List<Expression> expressions, Table table, String alias){
		ExpressionList<Expression> newExpressions = new ExpressionList<Expression>();
		for(Expression expression : expressions){
			EqualsTo _equalsTo = null;
			if(!(expression instanceof EqualsTo)){
				if(expression instanceof GreaterThan){
					GreaterThan greaterThan = (GreaterThan) expression;
					_equalsTo = new EqualsTo(greaterThan.getRightExpression(), greaterThan.getLeftExpression());
				}else if(expression instanceof MinorThan){
					MinorThan minorThan = (MinorThan) expression;
					_equalsTo = new EqualsTo(minorThan.getRightExpression(), minorThan.getLeftExpression());
				}else if(expression instanceof GreaterThanEquals){
					GreaterThanEquals greaterThanEquals = (GreaterThanEquals) expression;
					_equalsTo = new EqualsTo(greaterThanEquals.getRightExpression(), greaterThanEquals.getLeftExpression());
				}else if(expression instanceof MinorThanEquals){
					MinorThanEquals minorThanEquals = (MinorThanEquals) expression;
					_equalsTo = new EqualsTo(minorThanEquals.getRightExpression(), minorThanEquals.getLeftExpression());
				}else if(expression instanceof NotEqualsTo){
					NotEqualsTo notEqualsTo = (NotEqualsTo) expression;
					_equalsTo = new EqualsTo(notEqualsTo.getRightExpression(), notEqualsTo.getLeftExpression());
				}
			}else{
				_equalsTo = (EqualsTo) expression;
			}

			ParserHelper parserHelper = new ParserHelper();
			Column column = (Column) _equalsTo.getLeftExpression();
			if(parserHelper.areTablesEqual(column.getTable(),table)){
				newExpressions.add(new EqualsTo(column, new Column().withColumnName(column.getColumnName()).withTable(new Table(alias))));
			}else{
				 column = (Column) _equalsTo.getRightExpression();
				 newExpressions.add(new EqualsTo(new Column().withColumnName(column.getColumnName()).withTable(new Table(alias)), column));

			}
			
		}
		return newExpressions;		
	}

	/**
     * The function generates a String with the a SQL function to aggregegate values. For instance, in PostgreSQL is 'string_agg', for Trino is 'listaGG'. The expression is the column to be aggregated, the separator is the character to separate the values and the orderByColumn is the column to order the values.
     * 
     * @param expression the colum to be aggregated
     * @param separator the character to separate the values
     * @param orderByColumn the column to order the values
     * @return a String with the SQL Standard function 'ListAGG'
     */
    public String getAggFunction(String expression, char separator, String orderByColumn, String dbname){
		if(dbname.toLowerCase().compareTo("trino") == 0)
			if(orderByColumn == "")
        		return String.format("listagg(%s, ' %c ') WITHIN GROUP", expression, separator);
			else
				return String.format("listagg(%s, ' %c ') WITHIN GROUP (ORDER BY %s)", expression, separator, orderByColumn);
		else if(dbname.toLowerCase().compareTo("postgres") == 0)
			if(orderByColumn == "")
				return String.format("STRING_AGG(%s, ' %c ')", expression, separator, orderByColumn);
			else
				return String.format("STRING_AGG(%s, ' %c ' ORDER BY %s)", expression, separator, orderByColumn);
		
		return "";
    }

	/**
	 * A funciton to check if the query is a projection only with a function (Min, Max, Count, etc)
	 * @param selectItems the list of columns in the projection
	 * @return a boolean to indicate if the query is a projection only with a function
	 */
	public boolean projectionOnlyFunc(List<SelectItem<?>> selectItems) {
		boolean result = false;
		List<SelectItem<?>> newSelectItems = new ArrayList<SelectItem<?>>();

		for (SelectItem<?> selectItem : selectItems) {
			if(selectItem.getExpression() instanceof Column){
				if(selectItem.getAlias() != null && !selectItem.getAlias().getName().equals("prov")){
					newSelectItems.add(selectItem);
				}else if(selectItem.getAlias() == null && !((Column) selectItem.getExpression()).getColumnName().equals("prov")){
					newSelectItems.add(selectItem);
				}
			}else {
				newSelectItems.add(selectItem);
			}
		}

		if(newSelectItems.size() == 1){
			if(newSelectItems.get(0) instanceof SelectItem){
				SelectItem<?> selectItem = newSelectItems.get(0);
				if(selectItem.getExpression() instanceof Function || selectItem.getExpression() instanceof Multiplication || selectItem.getExpression() instanceof Division){
					result = true;
				}
			}
		}

		return result;
	}

	/**
	 * A function that constructs the column needed in the aggregation function for the order by
	 * @param firstColumn the first column in the projection
	 * @return a String with the column needed in the aggregation function for the order by
	 * @throws Exception
	 */
	public String aggFunctionOrderBy(SelectItem<?> firstColumn) throws Exception {
		if(firstColumn.getExpression() instanceof Column){
			Column column = (Column) firstColumn.getExpression();
			return column.getFullyQualifiedName();			
		}else{
			return "";
		} 
	}

	/**
	 * A function that returns the columns of a query in a list to apply the UNION rules
	 * 
	 * @param object the query
	 * @return a list of columns
	 */
	public List<SelectItem<?>> getUnionColumns(Object object){
		List<SelectItem<?>> result = new ArrayList<SelectItem<?>>();
		int count = 0;
		if(object instanceof PlainSelect){
			PlainSelect plainSelect = (PlainSelect) object;

			for(SelectItem<?> selectItem : plainSelect.getSelectItems()) {
				if(selectItem.getExpression() instanceof Function || selectItem.getExpression() instanceof Multiplication || selectItem.getExpression() instanceof Division || selectItem.getExpression() instanceof Addition || selectItem.getExpression() instanceof Subtraction){
					Column _column = null;
					if(selectItem.getAlias() == null){
						selectItem.setAlias(new Alias("func_"+count));
					}
					_column = new Column(new Table("_un"), selectItem.getAlias().getName());
					
					result.add(new SelectItem<Column>(_column));
				}else{
					Column _column = new Column(new Table("_un"), ((Column) selectItem.getExpression()).getColumnName());
					result.add(new SelectItem<Column>(_column));
				}
			}
		}else if(object instanceof ParenthesedSelect){
			ParenthesedSelect parenthesedSelect = (ParenthesedSelect) object;
			if(parenthesedSelect.getSelect() instanceof PlainSelect){
				PlainSelect plainSelect = (PlainSelect) parenthesedSelect.getSelect();
				
				for (SelectItem<?> selectItem : plainSelect.getSelectItems()) {
					if (selectItem.getExpression() instanceof Function) {
						if(selectItem.getAlias() == null){
							selectItem.setAlias(new Alias("func_"+count));
						}
						Column _column = new Column(new Table("_un"), selectItem.getAlias().getName());
						result.add(new SelectItem<Column>(_column));
					}else{
						Column _column = new Column(new Table("_un"), ((Column) selectItem.getExpression()).getColumnName());
						result.add(new SelectItem<Column>(_column));
					}
				}
			}else if(parenthesedSelect.getSelect() instanceof SetOperationList){
				result = getUnionColumns(((SetOperationList) parenthesedSelect.getSelect()).getSelects().get(0));
			} 
		}
			
		return result;
	}

}
