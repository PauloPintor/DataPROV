package com.generic.Parser;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

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

	public boolean areTablesEqual(Table table1, Table table2) {
		// Compare the table names
		String tableName1 = table1.getName();
		String tableName2 = table2.getName();

		// Compare the table aliases (if any)
		String alias1 = (table1.getAlias() != null) ? table1.getAlias().getName() : null;
		String alias2 = (table2.getAlias() != null) ? table2.getAlias().getName() : null;

		// Check if the names and aliases match
		return tableName1.equals(tableName2) && ((alias1 == null && alias2 == null) || alias1.equals(alias2)) || tableName1.equals(alias2) || tableName2.equals(alias1);
	}

		public List<Table> extractJoinTables(PlainSelect plainSelect ) {
        List<Table> joinTables = new ArrayList<>();

        try {
            // First, add the table from the main "FROM" clause
            FromItem mainFromItem = plainSelect.getFromItem();
            if (mainFromItem instanceof Table) {
                joinTables.add((Table) mainFromItem);
            }

            // Then, iterate over the join clauses and add join tables
            if (plainSelect.getJoins() != null) {
                for (Join join : plainSelect.getJoins()) {
                    FromItem joinFromItem = join.getRightItem();
                    if (joinFromItem instanceof Table) {
                        joinTables.add((Table) joinFromItem);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return joinTables;
    }


}
