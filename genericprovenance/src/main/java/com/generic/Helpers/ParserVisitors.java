package com.generic.Helpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ParserVisitors {
	private static int count = 0;

	public static class FunctionProjection extends SelectItemVisitorAdapter {
		private boolean hasCountProjection = false;

		@Override
		public void visit(SelectExpressionItem item)  {
			if(item.getExpression() instanceof Function){
				Function function = (Function) item.getExpression();

				function.accept(new ExpressionVisitorAdapter(){
					@Override
					public void visit(Column column) {
						if(column.getColumnName().equals("*")){
							hasCountProjection = true;
						}
					}
				});
				if(function.getName().equals("count")){
					hasCountProjection = true;
				}
			}
		}

		public boolean hasCountProjection() {
			return hasCountProjection;
		}
	
	}

	public static class ExistsVisitor extends ExpressionVisitorAdapter {
        private boolean hasExistsClause = false;
        //private HashMap<String, SubSelect> exists;
		private List<SubSelect> exists;
		private List<Expression> expressions;
		

		public ExistsVisitor(){
			//exists = new HashMap<>();
			exists = new ArrayList<>();
			expressions = new ArrayList<>();
		}

		@Override
		public void visit(AndExpression AndExpression) {
			AndExpression.getLeftExpression().accept(this);
			if(hasExistsClause)
				AndExpression.setLeftExpression(expressions.get(0));
			AndExpression.getRightExpression().accept(this);
			if(hasExistsClause)
				AndExpression.setRightExpression(expressions.get(0));
		}

		@Override
		public void visit(OrExpression OrExpression) {
			OrExpression.getLeftExpression().accept(this);
			OrExpression.getRightExpression().accept(this);
		}

        @Override
        public void visit(ExistsExpression existsExpression) {
            hasExistsClause = true;

			SubSelect existExp = (SubSelect) existsExpression.getRightExpression();
            

			if(existExp.getSelectBody() instanceof PlainSelect){
				PlainSelect plainSelect = (PlainSelect) existExp.getSelectBody();

				FromTables fromVisitor = new FromTables();				

				existExp.getSelectBody().accept(new SelectVisitorAdapter(){
					@Override
					public void visit(PlainSelect plainSelect) {
						plainSelect.getFromItem().accept(fromVisitor);
						if (plainSelect.getJoins()!=null)
						   plainSelect.getJoins().forEach(join -> join.getRightItem().accept(fromVisitor));
					}
				});

				ExistsWhereExp whereVisitor = new ExistsWhereExp(fromVisitor.tables);

				plainSelect.getWhere().accept(whereVisitor);
				expressions.addAll(whereVisitor.getExpressions());
			}

			existExp.setAlias(new Alias("C" + count));
			exists.add(existExp);
			//exists.put("C" + count, existExp);
			count++;		

			existsExpression = null;
        }

        public boolean hasExistsClause() {
            return hasExistsClause;
        }

		public List<Expression> getExpressions() {
			return expressions;
		}
        /*public HashMap<String,SubSelect> getSubSelects() {
            return exists;
        }*/

		public List<SubSelect> getSubSelects() {
			return exists;
		}
    }

	private static class FromTables extends FromItemVisitorAdapter {
		List<String> tables = new ArrayList<>();

		@Override
		public void visit(SubSelect subSelect) {
			//System.out.println("subselect=" + subSelect);
			tables.add(subSelect.getAlias().getName());
		}

		@Override
		public void visit(Table table) {
			//System.out.println("table=" + table);
			tables.add(table.getFullyQualifiedName());
		}
	}

	private static class ExistsWhereExp extends ExpressionVisitorAdapter{
		private List<Expression> expressions;
		private List<String> tables;

		public ExistsWhereExp(List<String> tables){
			this.tables = tables;
			expressions = new ArrayList<>();
		}

		public List<Expression> getExpressions() {
			return expressions;
		}

		@Override
		public void visit(EqualsTo equalsTo) {

			WhereColumns whereColumns = new WhereColumns(tables);

			equalsTo.getLeftExpression().accept(whereColumns);
			
			if(whereColumns.isOuterTable())
			{
				whereColumns.setIsOuterTable(true);
				equalsTo.getRightExpression().accept(whereColumns);

				expressions.add(new EqualsTo().withLeftExpression(equalsTo.getLeftExpression()).withRightExpression(equalsTo.getRightExpression()));

				equalsTo.setLeftExpression(new LongValue(1));
				equalsTo.setRightExpression(new LongValue(1));
			}else{
				equalsTo.getRightExpression().accept(whereColumns);
				if(whereColumns.isOuterTable())
				{
					whereColumns.setIsOuterTable(true);
					equalsTo.getLeftExpression().accept(whereColumns);

					expressions.add(new EqualsTo().withLeftExpression(equalsTo.getLeftExpression()).withRightExpression(equalsTo.getRightExpression()));
					
					equalsTo.setLeftExpression(new LongValue(1));
					equalsTo.setRightExpression(new LongValue(1));
				}
			}
		}

		/*@Override
		public void visit(GreaterThan equalsTo) {
			equalsTo.getLeftExpression().accept(new ExpressionVisitorAdapter(){
				@Override
				public void visit(Column column) {
					System.out.println("column=" + column);
				}
			});
			equalsTo.getRightExpression().accept(new ExpressionVisitorAdapter(){
				@Override
				public void visit(Column column) {
					System.out.println("column=" + column);
				}
			});
		}

		@Override
		public void visit(MinorThan equalsTo) {
			equalsTo.getLeftExpression().accept(new ExpressionVisitorAdapter(){
				@Override
				public void visit(Column column) {
					System.out.println("column=" + column);
				}
			});
			equalsTo.getRightExpression().accept(new ExpressionVisitorAdapter(){
				@Override
				public void visit(Column column) {
					System.out.println("column=" + column);
				}
			});
		}*/
	}

	private static class WhereColumns extends ExpressionVisitorAdapter{
		private boolean isOuterTable = false;
		private List<String> tables;

		public WhereColumns(List<String> tables){
			this.tables = tables;
		}

		@Override
		public void visit(Column column) {
			if(isOuterTable)
			{
				column.setTable(new Table("C"+count));
			}
			if(!tables.contains(column.getTable().getFullyQualifiedName())){
				isOuterTable = true;
			}
		}

		public boolean isOuterTable() {
            return isOuterTable;
        }

		public void setIsOuterTable(boolean isOuterTable) {
			this.isOuterTable = isOuterTable;
		}
	}
}


/*
 * FromItemVisitorAdapter fromVisitor = new FromItemVisitorAdapter() {
					List<String> tables = new ArrayList<>();
					@Override
					public void visit(SubSelect subSelect) {
						System.out.println("subselect=" + subSelect);
						tables.add(subSelect.getAlias().getName());
					}

					@Override
					public void visit(Table table) {
						System.out.println("table=" + table);
						tables.add(table.getFullyQualifiedName());
					}
				} ;
 */