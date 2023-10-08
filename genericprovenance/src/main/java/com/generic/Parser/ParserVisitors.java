package com.generic.Parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ParserVisitors {
	private static int count = 0;
	private static int inCount = 0;

	public static class FunctionProjection extends SelectItemVisitorAdapter {
		private boolean hasFunction = false;
		private String aggExpression = "";

		@Override
		public void visit(SelectExpressionItem item)  {
			if(item.getExpression() instanceof Function){
				Function function = (Function) item.getExpression();

				if(function.getName().toLowerCase().equals("count")){
					hasFunction = true;
					aggExpression = "|| ' x ' || CAST(1 as varchar)";
				}else if(function.getName().toLowerCase().equals("sum")){
					if(hasColumns(function.getParameters())){
						hasFunction = true;
						aggExpression += "|| ' x ' || CAST("+function.getParameters().toString()+" as varchar)";
					}
				}else if(function.getName().toLowerCase().equals("min")){
					if(hasColumns(function.getParameters())){
						hasFunction = true;
						aggExpression = "|| ' x ' || CAST("+function.getParameters().toString()+" as varchar)";
					}
				}

			}else if(item.getExpression() instanceof Multiplication){
				Multiplication multiplication = (Multiplication) item.getExpression();
				
				if(multiplication.getLeftExpression() instanceof Function){
					Function function = (Function) multiplication.getLeftExpression();
					if(function.getName().toLowerCase().equals("count")){
						hasFunction = true;
						aggExpression = "1";
					}else if(function.getName().toLowerCase().equals("sum")){
						if(hasColumns(function.getParameters())){
							aggExpression = "("+function.getParameters().toString()+")";
							hasFunction = true;
						}
					}
				}else if(multiplication.getLeftExpression() instanceof DoubleValue){
					aggExpression = multiplication.getLeftExpression().toString();
				}

				if(multiplication.getRightExpression() instanceof Function){
					Function function = (Function) multiplication.getRightExpression();
					if(function.getName().toLowerCase().equals("count")){
						aggExpression = aggExpression + "* 1";
						hasFunction = true;
					}else if(function.getName().toLowerCase().equals("sum")){
						if(hasColumns(function.getParameters())){
							aggExpression = aggExpression + "*" + "("+function.getParameters().toString()+")";
							hasFunction = true;
						}
					}
				}else if(multiplication.getLeftExpression() instanceof DoubleValue){
					aggExpression = aggExpression +"*"+ multiplication.getLeftExpression().toString();
				}

				aggExpression = "|| ' x ' || CAST("+aggExpression+" as varchar)";
			}
		}	

		private boolean hasColumns(ExpressionList parameters){
			for(Expression e : parameters.getExpressions()){
				if(e instanceof Column || e instanceof CaseExpression){
					return true;
				}else if(e instanceof Addition){
					Addition addition = (Addition) e;
					ExpressionList expressionList = new ExpressionList();
					expressionList.addExpressions(addition.getLeftExpression(), addition.getRightExpression());
					return hasColumns(expressionList);
				}else if(e instanceof Multiplication){
					Multiplication multiplication = (Multiplication) e;
					ExpressionList expressionList = new ExpressionList();
					expressionList.addExpressions(multiplication.getLeftExpression(), multiplication.getRightExpression());
					return hasColumns(expressionList);
				}
			}
			return false;
		}

		public boolean hasFunction() {
			return hasFunction;
		}

		public String getAggExpression() {
			return aggExpression;
		}
	}

	public static class ExistsVisitor extends ExpressionVisitorAdapter {
        private boolean hasExistsClause = false;
		private boolean hasInClause = false;
		private boolean hasAndClause = false;
		private boolean hasSubSelect = false;
        //private HashMap<String, SubSelect> exists;
		private List<SubSelect> exists;
		private List<Expression> expressions;
		private Expression inWhereExp = null;
		private List<String> tables;
		private List<String> inTables;
		private List<SubSelect> inselects;
		private List<Expression> inExpressions;
		private boolean _hasInClause = false;
		private boolean _hasExistsClause = false;
		private boolean _hasSubSelect = false;
		
		

		public ExistsVisitor(){
			//exists = new HashMap<>();
			exists = new ArrayList<>();
			expressions = new ArrayList<>();
			inExpressions = new ArrayList<>();
			inselects = new ArrayList<>();
		}


		private List<Table> extractJoinTables(PlainSelect plainSelect ) {
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

		public void processingOperators(PlainSelect plainSelect, ExistsVisitor existsVisitor){
			if(existsVisitor.hasInClause()){
				/*List<Join> joins = new ArrayList<>();
				for(String table : existsVisitor.getInTables()){
					Join newJoin = new Join();
					newJoin.setRightItem(new Table(table));
					newJoin.setLeft(true);
					newJoin.setOuter(true);
					List<Expression> inExpressions = new ArrayList<>();
					inExpressions.add(new EqualsTo(existsVisitor.getInExpressions().get(0),existsVisitor.getInExpressions().get(1)));
					newJoin.setOnExpressions(inExpressions);
					joins.add(newJoin);
					plainSelect.addJoins(joins);
				}*/
				if(existsVisitor.getInSubSelects().size() > 0){
				PlainSelect _plainSelect = new PlainSelect();
				List<Table> joinTables = extractJoinTables(plainSelect);
				List<Join> joins = new ArrayList<>();
				for(SubSelect select : existsVisitor.getInSubSelects()){
					List<Expression> inExpressions = new ArrayList<>();
					Column _col1 = (Column)existsVisitor.getInExpressions().get(0);
					Column _col2 = (Column)existsVisitor.getInExpressions().get(1);
					
					if(joinTables.stream().filter(t -> t.getFullyQualifiedName().equals(_col1.getTable().getFullyQualifiedName())).collect(Collectors.toList()).size() > 0){
						if(_plainSelect.getFromItem() == null){
							_plainSelect.setFromItem(_col1.getTable());
						}else{
							Join _newJoin = new Join();
							_newJoin.setRightItem(_col1.getTable());
							_newJoin.setSimple(true);
							joins.add(_newJoin);
							_plainSelect.addJoins(joins);
						}
						joinTables.removeAll(joinTables.stream().filter(t -> t.getFullyQualifiedName().equals(_col1.getTable().getFullyQualifiedName())).collect(Collectors.toList()));
					}else if(joinTables.stream().filter(t -> t.getFullyQualifiedName().equals(_col2.getTable().getFullyQualifiedName())).collect(Collectors.toList()).size() > 0){
						if(_plainSelect.getFromItem() == null){
							_plainSelect.setFromItem(_col2.getTable());
						}else{
							Join _newJoin = new Join();
							_newJoin.setRightItem(_col2.getTable());
							_newJoin.setSimple(true);
							joins.add(_newJoin);
							_plainSelect.addJoins(joins);
						}
						joinTables.removeAll(joinTables.stream().filter(t -> t.getFullyQualifiedName().equals(_col2.getTable().getFullyQualifiedName())).collect(Collectors.toList()));
					}

					Join newJoin = new Join();
					
					newJoin.setRightItem(select);
					newJoin.setLeft(true);
					newJoin.setOuter(true);

					inExpressions.add(new EqualsTo(existsVisitor.getInExpressions().get(0),existsVisitor.getInExpressions().get(1)));
					newJoin.setOnExpressions(inExpressions);
					joins.add(newJoin);

				}

				if(joinTables.size() > 0){
					for(Table table : joinTables){
						Join newJoin = new Join();
						newJoin.setRightItem(table);
						newJoin.setSimple(true);
						joins.add(newJoin);
						
					}
				}

				_plainSelect.addJoins(joins);
				plainSelect.setFromItem(_plainSelect.getFromItem());
				plainSelect.setJoins(_plainSelect.getJoins());
			}else{
				List<Join> joins = new ArrayList<>();
				for(String table : existsVisitor.getInTables()){
					Join newJoin = new Join();
					newJoin.setRightItem(new Table(table));
					newJoin.setLeft(true);
					newJoin.setOuter(true);
					List<Expression> inExpressions = new ArrayList<>();
					inExpressions.add(new EqualsTo(existsVisitor.getInExpressions().get(0),existsVisitor.getInExpressions().get(1)));
					newJoin.setOnExpressions(inExpressions);
					joins.add(newJoin);
					plainSelect.addJoins(joins);
				}
			}

	
				if(existsVisitor.hasAndClause() && existsVisitor.getInWhereExp() != null){
					Expression where = plainSelect.getWhere();
					where = new AndExpression(where, existsVisitor.getInWhereExp());
					plainSelect.setWhere(where);
				}else if(!existsVisitor.hasAndClause()){
					plainSelect.setWhere(new EqualsTo(new LongValue(1),new LongValue(1)));
					if(existsVisitor.getInWhereExp() != null) plainSelect.setWhere(existsVisitor.getInWhereExp());
				}
			}
	
			if(existsVisitor.hasExistsClause() || existsVisitor.hasSubSelect()){	
				List<Join> joins = new ArrayList<>();
				for(SubSelect select : existsVisitor.getSubSelects()){
					Join newJoin = new Join();
					newJoin.setRightItem(select);
					newJoin.setSimple(true);
					joins.add(newJoin);
					plainSelect.addJoins(joins);
				}
	
				if(existsVisitor.hasAndClause() && existsVisitor.getExpressions().size() > 1){
					List<Expression> expressions = existsVisitor.getExpressions();
					//TODO: Check if this is correct or what is the query wrong (Corrected to the query 20)
					//expressions.remove(0);
					Expression where = plainSelect.getWhere();
					for(Expression exp : expressions){
						where = new AndExpression(where, exp);
					}
	
					plainSelect.setWhere(where);
				}else if(existsVisitor.hasSubSelect() && existsVisitor.getExpressions().size() > 0){
					List<Expression> expressions = existsVisitor.getExpressions();
					Expression where = plainSelect.getWhere();
					for(Expression exp : expressions){
						where = new AndExpression(where, exp);
					}
	
					plainSelect.setWhere(where);
				}else if(!existsVisitor.hasAndClause() && !existsVisitor.hasSubSelect()){
					plainSelect.setWhere(new EqualsTo(new LongValue(1),new LongValue(1)));
					if(existsVisitor.getExpressions().size() > 0){
						Expression where = plainSelect.getWhere();
						for(Expression exp : existsVisitor.getExpressions()){
							where = new AndExpression(where, exp);
						}
	
						plainSelect.setWhere(where);
					}
				}
			}
		}

		@Override
		public void visit(AndExpression AndExpression) {
			hasAndClause = true;
			AndExpression.getLeftExpression().accept(this);
			if(_hasExistsClause){
				AndExpression.setLeftExpression(expressions.get(0));
				_hasExistsClause = false;
			}
			if(_hasInClause){
				if(AndExpression.getRightExpression() instanceof InExpression){
					InExpression in = (InExpression) AndExpression.getRightExpression();
					if(in.getLeftExpression() instanceof Column){
						inExpressions.add(in.getLeftExpression());
					}else
						inExpressions.add(in.getRightExpression());
				}else if(AndExpression.getLeftExpression() instanceof InExpression){
					InExpression in = (InExpression) AndExpression.getLeftExpression();
					if(in.getLeftExpression() instanceof Column){
						inExpressions.add(in.getLeftExpression());
					}else
						inExpressions.add(in.getRightExpression());
				}
				AndExpression.setLeftExpression(new EqualsTo(new LongValue(1), new LongValue(1)));
				_hasInClause = false;
			}
			AndExpression.getRightExpression().accept(this);
			if(_hasExistsClause){
				AndExpression.setRightExpression(expressions.get(0));
				_hasExistsClause = false;
			}
			if(_hasInClause){
				if(AndExpression.getRightExpression() instanceof InExpression){
					InExpression in = (InExpression) AndExpression.getRightExpression();
					if(in.getLeftExpression() instanceof Column){
						inExpressions.add(in.getLeftExpression());
					}else
						inExpressions.add(in.getRightExpression());
				}else if(AndExpression.getLeftExpression() instanceof InExpression){
					InExpression in = (InExpression) AndExpression.getLeftExpression();
					if(in.getLeftExpression() instanceof Column){
						inExpressions.add(in.getLeftExpression());
					}else
						inExpressions.add(in.getRightExpression());
				}
				AndExpression.setRightExpression(new EqualsTo(new LongValue(1), new LongValue(1)));
				_hasInClause = false;
			}
		}

		@Override
		public void visit(OrExpression OrExpression) {
			OrExpression.getLeftExpression().accept(this);
			OrExpression.getRightExpression().accept(this);
		}

		@Override
		public void visit(GreaterThan greaterThan) {
			greaterThan.getLeftExpression().accept(this);
			if(hasSubSelect){
				if(exists.get(count - 1).getSelectBody() instanceof PlainSelect){
					PlainSelect plainSelect = (PlainSelect) exists.get(count - 1).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					greaterThan.setRightExpression(new Column("C"+(count-1)+"."+selectExpressionItem.getAlias().getName()));
				}
			}
			greaterThan.getRightExpression().accept(this);
			if(hasSubSelect){
				if(exists.get(count - 1).getSelectBody() instanceof PlainSelect){
					PlainSelect plainSelect = (PlainSelect) exists.get(count - 1).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					
					String cName = "";
					if(selectExpressionItem.getAlias() == null){
						Column column = (Column) selectExpressionItem.getExpression();
						cName = column.getColumnName();
					}else
						cName = selectExpressionItem.getAlias().getName();

					greaterThan.setRightExpression(new Column("C"+(count-1)+"."+cName));
				}
			}
		}

		@Override
		public void visit(EqualsTo equalsTo) {
			equalsTo.getLeftExpression().accept(this);
			if(hasSubSelect){
				if(exists.get(count - 1).getSelectBody() instanceof PlainSelect){
					PlainSelect plainSelect = (PlainSelect) exists.get(count - 1).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					equalsTo.setRightExpression(new Column("C"+(count-1)+"."+selectExpressionItem.getAlias().getName()));
				}
			}
			equalsTo.getRightExpression().accept(this);
			if(hasSubSelect){
				if(exists.get(count - 1).getSelectBody() instanceof PlainSelect){
					PlainSelect plainSelect = (PlainSelect) exists.get(count - 1).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					equalsTo.setRightExpression(new Column("C"+(count-1)+"."+selectExpressionItem.getAlias().getName()));
				}
			}
		}

		@Override
		public void visit(SubSelect subSelect) {
			hasSubSelect = true;
			_hasSubSelect = true;
			boolean minMax = false;

			if(subSelect.getSelectBody() instanceof PlainSelect){
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();

				FromTables fromVisitor = new FromTables();				

				subSelect.getSelectBody().accept(new SelectVisitorAdapter(){
					@Override
					public void visit(PlainSelect plainSelect) {
						plainSelect.getFromItem().accept(fromVisitor);
						if (plainSelect.getJoins()!=null)
						   plainSelect.getJoins().forEach(join -> join.getRightItem().accept(fromVisitor));
					}
				});

				tables = fromVisitor.tables;
				
				ExistsWhereExp whereVisitor = new ExistsWhereExp(fromVisitor.tables, true);
				if(plainSelect.getWhere() != null){
					plainSelect.getWhere().accept(whereVisitor);
					expressions.addAll(whereVisitor.getExpressions());
				}

				if(plainSelect.getSelectItems().size() == 1){
					if(plainSelect.getSelectItems().get(0) instanceof SelectExpressionItem){
						SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
						if(!(selectExpressionItem.getExpression() instanceof Function || selectExpressionItem.getExpression() instanceof Multiplication)){
							plainSelect.addGroupByColumnReference(selectExpressionItem.getExpression());
							Column _column = (Column) selectExpressionItem.getExpression();
							if(!whereVisitor.getColumns().contains(_column)){
								List<Column> temp = new ArrayList<>();
								temp.add(_column);
								temp.addAll(whereVisitor.getColumns());
								whereVisitor.setColumns(temp);
							}
						}
						else{
							if(selectExpressionItem.getAlias() == null){
								selectExpressionItem.setAlias(new Alias("CL" + count));
							}
							if(selectExpressionItem.getExpression() instanceof Function){
								Function function = (Function) selectExpressionItem.getExpression();
								if(function.getName().toLowerCase().equals("min")){
									minMax = true;
								}else if(function.getName().toLowerCase().equals("max")){
									minMax = true;
								}
							}
						}						
					}
				}

				for(Column c : whereVisitor.getColumns()){
					if(!plainSelect.getSelectItems().contains(new SelectExpressionItem(c))){
						plainSelect.addSelectItems(new SelectExpressionItem(c));
						plainSelect.addGroupByColumnReference(new SelectExpressionItem(c).getExpression());
					}
				}

				List<Join> _join = new ArrayList<>();
				if(minMax){
					SubSelect _subSelect = new SubSelect();
					//PlainSelect _plainSelect = new PlainSelect();
					//_plainSelect = plainSelect;
					// Step 2: Create a new SelectItem (column to be selected)
					SelectExpressionItem selectItem = new SelectExpressionItem();
					selectItem.setExpression(new Column("column1"));
			
					// Step 3: Add the SelectItem to the SelectItems list
					PlainSelect _plainSelect = new PlainSelect();
					_plainSelect.addSelectItems(plainSelect.getSelectItems());

					_plainSelect.setFromItem(plainSelect.getFromItem());
					if(plainSelect.getJoins() != null) _plainSelect.addJoins(plainSelect.getJoins());
					if(plainSelect.getWhere() != null) _plainSelect.setWhere(plainSelect.getWhere());
					if(plainSelect.getGroupBy() != null) _plainSelect.setGroupByElement(plainSelect.getGroupBy());

					_subSelect.setSelectBody(_plainSelect);
					_subSelect.setAlias(new Alias("MinMax" + count));
					Join _newJoin = new Join();
					_newJoin.setRightItem(_subSelect);
					_newJoin.setSimple(true);		
					_join.add(_newJoin);
						
					plainSelect.addJoins(_join);
				

					for (SelectItem _selectItem : plainSelect.getSelectItems()) {
						if (_selectItem instanceof SelectExpressionItem) {
							SelectExpressionItem selectExpressionItem = (SelectExpressionItem) _selectItem;
							
							if(selectExpressionItem.getExpression() instanceof Function){
								EqualsTo equalsTo = new EqualsTo();
								equalsTo.setLeftExpression(new Column(selectExpressionItem.getAlias().getName()));
								equalsTo.setRightExpression(new Column(new Table("MinMax" + count),selectExpressionItem.getAlias().getName()));
								
								Expression where = plainSelect.getWhere();
								if(where == null)
									plainSelect.setWhere(equalsTo);
								else{
									where = new AndExpression(where, equalsTo);
									plainSelect.setWhere(where);
								}
							}else{
								Column column = (Column) selectExpressionItem.getExpression();
								EqualsTo equalsTo = new EqualsTo();
								equalsTo.setLeftExpression(column);
								Column newColumn = new Column(new Table("MinMax" + count), column.getColumnName());
								equalsTo.setRightExpression(newColumn);
								
								Expression where = plainSelect.getWhere();
								where = new AndExpression(where, equalsTo);
								plainSelect.setWhere(where);
							}						
						}            
					}
				}

				subSelect.setAlias(new Alias("C" + count));
				exists.add(subSelect);
				count++;	
			}
		}


		@Override
        public void visit(InExpression inExpression) {

			SubSelect existExp = null;

			if((inExpression.getLeftExpression() instanceof SubSelect)) {
				existExp = (SubSelect) inExpression.getLeftExpression();
			}else if((inExpression.getRightExpression() instanceof SubSelect)) {
				existExp = (SubSelect) inExpression.getRightExpression();
			}

			if(existExp == null) return;
			
			hasInClause = true;
			_hasInClause = true;

			
			

			if(existExp.getSelectBody() instanceof PlainSelect){
				PlainSelect plainSelect = (PlainSelect) existExp.getSelectBody();

				//TODO falta ver quando o dentro do subquery existem clausulas do WHERE sem SUBSELECT pois são precisas passar para fora

				if(plainSelect.getWhere() != null){
					ExistsVisitor existsVisitor = new ExistsVisitor();
					plainSelect.getWhere().accept(existsVisitor);
					processingOperators(plainSelect, existsVisitor);
				}				

				FromTables fromVisitor = new FromTables();				

				existExp.getSelectBody().accept(new SelectVisitorAdapter(){
					@Override
					public void visit(PlainSelect plainSelect) {
						plainSelect.getFromItem().accept(fromVisitor);
						if (plainSelect.getJoins()!=null)
						   plainSelect.getJoins().forEach(join -> join.getRightItem().accept(fromVisitor));
					}
				});

				Column _column = null;
				if(plainSelect.getSelectItems().get(0) instanceof SelectExpressionItem){
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					if(!(selectExpressionItem.getExpression() instanceof Function || selectExpressionItem.getExpression() instanceof Multiplication)){
						plainSelect.addGroupByColumnReference(selectExpressionItem.getExpression());
						_column = (Column) selectExpressionItem.getExpression();
						_column = new Column().withColumnName(_column.getColumnName()).withTable(new Table("CI" + inCount));							
						inExpressions.add(_column);
					}
				}
				if(_column != null){
					IsNullExpression notNullExp = new IsNullExpression();
					notNullExp.setLeftExpression(_column);
					notNullExp.setNot(true);
					inWhereExp = notNullExp;
				}
				existExp.setAlias(new Alias("CI" + inCount));
				inselects.add(existExp);
				inCount++;

			}
		}

        @Override
        public void visit(ExistsExpression existsExpression) {
            hasExistsClause = true;
			_hasExistsClause = true;

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

				ExistsWhereExp whereVisitor = new ExistsWhereExp(fromVisitor.tables, true);

				plainSelect.getWhere().accept(whereVisitor);
				expressions.addAll(whereVisitor.getExpressions());

				SelectItem firstSelectItem = plainSelect.getSelectItems().get(0);
			
				if (firstSelectItem instanceof AllColumns) {
					List<SelectItem> selectItems = new ArrayList<>();
					for(Column c : whereVisitor.getColumns())
						selectItems.add(new SelectExpressionItem(c));
					plainSelect.setSelectItems(selectItems);
				}

				 GroupByElement newGroupByElement = new GroupByElement();
				 newGroupByElement.addGroupByExpressions(whereVisitor.getColumns());
				 plainSelect.setGroupByElement(newGroupByElement);

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
        
		public boolean hasInClause() {
			return hasInClause;
		}

		public List<String> getInTables() {
			return inTables;
		}

		public List<SubSelect> getSubSelects() {
			return exists;
		}

		public List<SubSelect> getInSubSelects() {
			return inselects;
		}

		public boolean hasAndClause() {
			return hasAndClause;
		}

		public boolean hasSubSelect() {
			return hasSubSelect;
		}

		public Expression getInWhereExp() {
			return inWhereExp;
		}

		public List<Expression> getInExpressions() {
			return inExpressions;
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
		private List<Column> columns;
		private Boolean alias;

		public ExistsWhereExp(List<String> tables, Boolean alias){
			this.tables = tables;
			expressions = new ArrayList<>();
			this.alias = alias;
			this.columns = new ArrayList<>();
		}

		public List<Expression> getExpressions() {
			return expressions;
		}

		@Override
		public void visit(EqualsTo equalsTo) {

			WhereColumns whereColumns = new WhereColumns(tables, alias);

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

			columns.addAll(whereColumns.getColumns());
		}

		public List<Column> getColumns() {
			return columns;
		}

		public void setColumns(List<Column> columns) {
			this.columns = columns;
		}
	}

	private static class WhereColumns extends ExpressionVisitorAdapter{
		private boolean isOuterTable = false;
		private List<String> tables;
		private List<Column> columns;
		private Boolean alias;

		public WhereColumns(List<String> tables, Boolean alias){
			this.tables = tables;
			this.columns = new ArrayList<>();
			this.alias = alias;
		}

		@Override
		public void visit(Column column) {
			if(isOuterTable)
			{
				columns.add(new Column().withColumnName(column.getColumnName()).withTable(new Table(column.getTable().getFullyQualifiedName())));
				if(alias) column.setTable(new Table("C"+count));
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

		public List<Column> getColumns() {
			return columns;
		}
	}
}