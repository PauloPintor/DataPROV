package com.generic.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.AnyType;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
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
		private boolean minMax = false;
		private Column minMaxColumn = null;

		@Override
		public void visit(SelectExpressionItem item)  {
			if(item.getExpression() instanceof Function){
				Function function = (Function) item.getExpression();

				if(function.getName().toLowerCase().equals("count")){
					hasFunction = true;
					aggExpression = "|| ' . ' || CAST(1 as varchar)";
				}else if(function.getName().toLowerCase().equals("sum")){
					if(hasColumns(function.getParameters())){
						hasFunction = true;
						aggExpression += "|| ' . ' || CAST("+function.getParameters().toString()+" as varchar)";
					}
				}else if(function.getName().toLowerCase().equals("min")){
					if(hasColumns(function.getParameters())){
						function.getParameters().getExpressions().forEach(e -> {
							if(e instanceof Column){
								minMaxColumn = (Column) e;
							}
						});

						hasFunction = true;
						minMax = true;
						aggExpression = "|| ' . ' || CAST("+function.getParameters().toString()+" as varchar)";
					}
				}else if(function.getName().toLowerCase().equals("max")){
					if(hasColumns(function.getParameters())){
						function.getParameters().getExpressions().forEach(e -> {
							if(e instanceof Column){
								minMaxColumn = (Column) e;
							}
						});
						
						hasFunction = true;
						minMax = true;
						aggExpression = "|| ' . ' || CAST("+function.getParameters().toString()+" as varchar)";
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

				aggExpression = "|| ' . ' || CAST("+aggExpression+" as varchar)";
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

		public boolean isMinMax() {
			return minMax;
		}

		public Column getMinMaxColumn() {
			return minMaxColumn;
		}

		public boolean hasFunction() {
			return hasFunction;
		}

		public String getAggExpression() {
			return aggExpression;
		}
	}

	public static class WhereVisitor extends ExpressionVisitorAdapter{
		boolean hasExists = false;
		private List<ParserExpression> parserExpressions;
		private Expression newWhere = null;
		private Expression _newJoin = null;
		private List<Column> _columns = null;
		private List<Table> mainQueryTables = null;
		private boolean isInAndExpression = false;
		private boolean isInOrExpression = false;

		public WhereVisitor(){
			parserExpressions = new ArrayList<>();
			_columns = new ArrayList<>();
		}

		public WhereVisitor(List<Table> mainQueryTables){
			parserExpressions = new ArrayList<>();
			_columns = new ArrayList<>();
			this.mainQueryTables = mainQueryTables;
		}

		public PlainSelect identifyOperators(PlainSelect plainSelect){
			ParserHelper ph = new ParserHelper();
			mainQueryTables = ph.extractJoinTables(plainSelect);
			WhereVisitor whereVisitor = new WhereVisitor(mainQueryTables);
			plainSelect.getWhere().accept(whereVisitor);
			
			if(whereVisitor.getParserExpressions().size() > 0)
				processingOperators(plainSelect, whereVisitor);

			return plainSelect;
		} 

		private PlainSelect processingOperators(PlainSelect plainSelect, WhereVisitor whereVisitor){
			ParserHelper ph = new ParserHelper();
			List<Join> joins = null;
			List<Expression> expressions = null;

			for(ParserExpression pe : whereVisitor.getParserExpressions())
			{
				joins = new ArrayList<>();
				expressions = new ArrayList<>();

				if(pe.getJoinTable() == null){
					Join newJoin = new Join();
					newJoin.setRightItem(pe.getSelect());
					newJoin.setSimple(true);
					if(plainSelect.getJoins() != null)
						joins.addAll(plainSelect.getJoins());
					joins.add(newJoin);

					plainSelect.setJoins(joins);

					if(whereVisitor.getNewWhere() == null)
						plainSelect.setWhere(ph.buildAndExpression(pe.getWhereExpressions()));
					else
						plainSelect.setWhere(new AndExpression(whereVisitor.getNewWhere(), ph.buildAndExpression(pe.getWhereExpressions())));
					
				}else{
					if(ph.areTblEqualsTblSelect(pe.getJoinTable(), plainSelect.getFromItem()))
					{
						addNewJoin(pe, joins, expressions);
						
						if(plainSelect.getJoins() != null)
							joins.addAll(plainSelect.getJoins());
							
					}else{
						for(Join j : plainSelect.getJoins()){
							joins.add(j);
							if(ph.areTblEqualsTblSelect(pe.getJoinTable(), j.getRightItem())){
								addNewJoin(pe, joins, expressions);
							}
						}
					}

					plainSelect.setJoins(joins);

					plainSelect.setWhere(whereVisitor.getNewWhere());
					
					if(expressions.size() > 0){
						Expression where = plainSelect.getWhere();
						
						if(where == null){
							where = expressions.get(0);
							expressions.remove(0);
						}
						else 
							for(Expression exp : expressions){
								where = new AndExpression(where, exp);
							}

						plainSelect.setWhere(where);
					}
				}
			}
			return plainSelect;
		}

		private void addNewJoin(ParserExpression pe, List<Join> joins, List<Expression> expressions){
			ParserHelper ph = new ParserHelper();
			Join newJoin = new Join();
							
			newJoin.setRightItem(pe.getSelect());

			if(pe.HasWhereExp()){
				newJoin.setLeft(true);
				newJoin.setOuter(false);
			}else{
				newJoin.setLeft(false);
				newJoin.setOuter(false);
			}

			List<Expression> joinExp = new ArrayList<>();
			joinExp.add(ph.buildAndExpression(pe.getJoinExpression()));
			newJoin.setOnExpressions(joinExp);
			joins.add(newJoin);

			if(pe.HasWhereExp())
				expressions.addAll(pe.getWhereExpressions());
		}

		@Override
		public void visit(NotExpression notExistsExpression) {
		
			if(notExistsExpression.getExpression() instanceof ExistsExpression){

				ExistsExpression existsExpression = (ExistsExpression) notExistsExpression.getExpression();
				SubSelect existExp = (SubSelect) existsExpression.getRightExpression();

				if(existExp.getSelectBody() instanceof PlainSelect){
					List<Table> tables = new ArrayList<>();
					ParserHelper ph = new ParserHelper();

					PlainSelect plainSelect = (PlainSelect) existExp.getSelectBody();

					if(plainSelect.getWhere() != null){
						WhereVisitor whereVisitor = new WhereVisitor();
						plainSelect.getWhere().accept(whereVisitor);
						if(whereVisitor.getParserExpressions().size() > 0)
							plainSelect = whereVisitor.processingOperators(plainSelect, whereVisitor);
					}

					tables = ph.extractJoinTables(plainSelect);
				
					ColumnsInvolved columnsInvolved = new ColumnsInvolved(tables);
					plainSelect.getWhere().accept(columnsInvolved);
					plainSelect.setWhere(columnsInvolved.getNewWhere());

					SelectItem firstSelectItem = plainSelect.getSelectItems().get(0);
					ParserExpression pe = new ParserExpression();
					if (firstSelectItem instanceof AllColumns) {
						List<SelectItem> selectItems = new ArrayList<>();
						for(Column c : columnsInvolved.getColumns()){
							selectItems.add(new SelectExpressionItem(c));
						}
						plainSelect.setSelectItems(selectItems);
					}

					for(Column c : columnsInvolved.getColumns()){
						IsNullExpression notNullExp = new IsNullExpression();
						Column colWhere = new Column();
						colWhere.setColumnName(c.getColumnName());
						colWhere.setTable(new Table("C" + count));
						notNullExp.setLeftExpression(colWhere);
						notNullExp.setNot(false);
						pe.addWhereExpression(notNullExp);
					}

					existExp.setAlias(new Alias("C" + count));
					
					pe.setSelect(existExp);
					pe.setJoinTable(columnsInvolved.getJoinTable());
					pe.setJoinExpressions(columnsInvolved.getExpressions());
					parserExpressions.add(pe);
					count++;
				}
			}
		}
	
		@Override
		public void visit(ExistsExpression existsExpression) {
			hasExists = true;
			SubSelect existExp = (SubSelect) existsExpression.getRightExpression();
            
			if(existExp.getSelectBody() instanceof PlainSelect){
				List<Table> tables = new ArrayList<>();
				ParserHelper ph = new ParserHelper();
			
				PlainSelect plainSelect = (PlainSelect) existExp.getSelectBody();

				if(plainSelect.getWhere() != null){
					WhereVisitor whereVisitor = new WhereVisitor();
					plainSelect.getWhere().accept(whereVisitor);
					if(whereVisitor.getParserExpressions().size() > 0)
						plainSelect = whereVisitor.processingOperators(plainSelect, whereVisitor);
				}

				tables = ph.extractJoinTables(plainSelect);
				
				ColumnsInvolved columnsInvolved = new ColumnsInvolved(tables);
				plainSelect.getWhere().accept(columnsInvolved);
				plainSelect.setWhere(columnsInvolved.getNewWhere());

				SelectItem firstSelectItem = plainSelect.getSelectItems().get(0);
			
				if (firstSelectItem instanceof AllColumns) {
					List<SelectItem> selectItems = new ArrayList<>();
					for(Column c : columnsInvolved.getColumns())
						selectItems.add(new SelectExpressionItem(c));
					plainSelect.setSelectItems(selectItems);
				}

				ParserExpression pe = new ParserExpression();
				PlainSelect _outerJoin = new PlainSelect();

				Table _table = columnsInvolved.getJoinTable();

				for(Table t : mainQueryTables){
					if(t.getAlias() != null && t.getAlias().getName().equals(_table.getName()))
						_table = t;
				}

				_outerJoin.setFromItem(_table);
				Join newJoin = new Join();
				
				List<SelectItem> selectItems = new ArrayList<>();
				for(Column c : columnsInvolved.getCorrelatedColums())
					selectItems.add(new SelectExpressionItem(c));
				_outerJoin.setSelectItems(selectItems);

				// Create a new SubSelect
				SubSelect subSelect = new SubSelect();

				// Set the PlainSelect as the subquery
				subSelect.setSelectBody(plainSelect);
				subSelect.setAlias(new Alias("C" + count));
				newJoin.setRightItem (subSelect);
				newJoin.setLeft(false);
				newJoin.setOuter(false);
					
				List<Expression> joinExp = new ArrayList<>();
				
				joinExp.add(ph.buildAndExpression(columnsInvolved.getExpressions()));
				newJoin.setOnExpressions(joinExp);
				_outerJoin.addJoins(newJoin);

				GroupByElement newGroupByElement = new GroupByElement();
				newGroupByElement.addGroupByExpressions(columnsInvolved.getCorrelatedColums());
				_outerJoin.setGroupByElement(newGroupByElement);

				SubSelect _subSelect = new SubSelect();
				_subSelect.setSelectBody(_outerJoin);
				_subSelect.setAlias(new Alias("nestedT" + count));
				
				pe.setSelect(_subSelect);
				pe.setJoinTable(columnsInvolved.getJoinTable());
				pe.setJoinExpressions(ph.equiJoins(columnsInvolved.getExpressions(), _table,"nestedT" + count));

				parserExpressions.add(pe);

				count++;
			}					
		}

		@Override
		public void visit(InExpression inExpression){
			SubSelect inExp = null;
			List<Column> inColumns = new ArrayList<>();

			if((inExpression.getLeftExpression() instanceof SubSelect)) {
				inExp = (SubSelect) inExpression.getLeftExpression();
				if(inExpression.getRightExpression() instanceof RowConstructor){
					((RowConstructor) inExpression.getRightExpression()).getExprList().getExpressions().forEach(e -> {
						if(e instanceof Column)
							inColumns.add((Column) e);
					});
				}else if (inExpression.getRightExpression() instanceof Column){
					inColumns.add((Column) inExpression.getRightExpression());
				}
					
			}else{
				inExp = (SubSelect) inExpression.getRightExpression();
				if(inExpression.getLeftExpression() instanceof RowConstructor){
					((RowConstructor) inExpression.getLeftExpression()).getExprList().getExpressions().forEach(e -> {
						if(e instanceof Column)
							inColumns.add((Column) e);
					});
				}else if (inExpression.getLeftExpression() instanceof Column){
					inColumns.add((Column) inExpression.getLeftExpression());
				}
			}
			
			if(inExp.getSelectBody() instanceof PlainSelect){
				List<Table> tables = new ArrayList<>();
				ParserHelper ph = new ParserHelper();
			
				PlainSelect plainSelect = (PlainSelect) inExp.getSelectBody();

				if(plainSelect.getWhere() != null){
					WhereVisitor whereVisitor = new WhereVisitor();
					plainSelect.getWhere().accept(whereVisitor);
					if(whereVisitor.getParserExpressions().size() > 0)
						plainSelect = whereVisitor.processingOperators(plainSelect, whereVisitor);
				}

				tables = ph.extractJoinTables(plainSelect);
				
				ColumnsInvolved columnsInvolved = new ColumnsInvolved(tables);
				if(plainSelect.getWhere() != null){
					plainSelect.getWhere().accept(columnsInvolved);
					plainSelect.setWhere(columnsInvolved.getNewWhere());
				}

				ParserExpression pe = new ParserExpression();
				

				Table _table = columnsInvolved.getJoinTable();

				if(_table == null && inColumns.size() == 1)
				{
					// Create a new SubSelect
					SubSelect subSelect = new SubSelect();

					// Set the PlainSelect as the subquery
					subSelect.setSelectBody(plainSelect);
					subSelect.setAlias(new Alias("C" + count));
					Column inColumn = new Column();
					GroupByElement newGroupByElement = new GroupByElement();
					for(SelectItem si : plainSelect.getSelectItems()){
						SelectExpressionItem sei = (SelectExpressionItem) si;
						inColumn.setColumnName(((Column) sei.getExpression()).getColumnName());
						inColumn.setTable(new Table("C" + count));
						newGroupByElement.addGroupByExpressions(sei.getExpression());
					}

					if(inExpression.isNot()){
						for(Column c : columnsInvolved.getColumns()){
							IsNullExpression notNullExp = new IsNullExpression();
							Column colWhere = new Column();
							colWhere.setColumnName(c.getColumnName());
							colWhere.setTable(new Table("C" + count));
							notNullExp.setLeftExpression(colWhere);
							notNullExp.setNot(false);
							pe.addWhereExpression(notNullExp);
						}

						for(Column c : inColumns){
							IsNullExpression notNullExp = new IsNullExpression();
							Column colWhere = new Column();
							colWhere.setColumnName(c.getColumnName());
							colWhere.setTable(new Table("C" + count));
							notNullExp.setLeftExpression(colWhere);
							notNullExp.setNot(false);
							pe.addWhereExpression(notNullExp);
						}
					}
					
					plainSelect.setGroupByElement(newGroupByElement);

					pe.setSelect(subSelect);
					pe.setJoinTable(inColumns.get(0).getTable());
					pe.setJoinExpressions(new EqualsTo(inColumns.get(0), inColumn));
				}else{
					PlainSelect _outerJoin = new PlainSelect();
					_table = _table == null ? inColumns.get(0).getTable() : _table;
					_outerJoin.setFromItem(_table);
					Join newJoin = new Join();
					
					List<SelectItem> selectItems = new ArrayList<>();
					for(Column c : columnsInvolved.getCorrelatedColums())
						selectItems.add(new SelectExpressionItem(c));

					for(Column c : inColumns)
						selectItems.add(new SelectExpressionItem(c));
					_outerJoin.setSelectItems(selectItems);
					List<Expression> _joinExp = new ArrayList<>();

					// Create a new SubSelect
					SubSelect subSelect = new SubSelect();
					List<SelectItem> tempSelectItems = new ArrayList<>();
					selectItems = plainSelect.getSelectItems();
					int i = 0;
					for(SelectItem item : selectItems){
						
						SelectExpressionItem sei = (SelectExpressionItem) item;
						if(sei.getExpression() instanceof Column){
							Column _c = (Column) sei.getExpression();
							Column _newC = new Column(_c.getColumnName());
							_newC.setTable(new Table("C" + count));

							_joinExp.add(new EqualsTo(_newC, inColumns.get(i)));

							for(Column c : columnsInvolved.getColumns())
								if(!ph.areColumnsEquals(c, _c))
									tempSelectItems.add(new SelectExpressionItem(c));
						}
						i++;
					}
					selectItems.addAll(tempSelectItems);
					
					plainSelect.setSelectItems(selectItems);
					
					// Set the PlainSelect as the subquery
					subSelect.setSelectBody(plainSelect);
					subSelect.setAlias(new Alias("C" + count));
					newJoin.setRightItem (subSelect);
					newJoin.setLeft(false);
					newJoin.setOuter(false);
						
					if(columnsInvolved.getExpressions() != null)
						_joinExp.addAll(columnsInvolved.getExpressions());

					List<Expression> joinExp = new ArrayList<>();
					joinExp.add(ph.buildAndExpression(_joinExp));
					newJoin.setOnExpressions(joinExp);
					_outerJoin.addJoins(newJoin);

					GroupByElement newGroupByElement = new GroupByElement();
					newGroupByElement.addGroupByExpressions(columnsInvolved.getCorrelatedColums());
					newGroupByElement.addGroupByExpressions(inColumns);
					_outerJoin.setGroupByElement(newGroupByElement);

					SubSelect _subSelect = new SubSelect();
					_subSelect.setSelectBody(_outerJoin);
					_subSelect.setAlias(new Alias("nestedT" + count));
					
					pe.setSelect(_subSelect);
					pe.setJoinTable(columnsInvolved.getJoinTable() == null ? inColumns.get(0).getTable(): columnsInvolved.getJoinTable());
					pe.setJoinExpressions(ph.equiJoins(_joinExp, _table,"nestedT" + count));
				}

				parserExpressions.add(pe);

				count++;
			}
		}

		@Override
		public void visit(AnyComparisonExpression anyExpression){
			SubSelect existExp = (SubSelect) anyExpression.getSubSelect();
            
			if(existExp.getSelectBody() instanceof PlainSelect){
				List<Table> tables = new ArrayList<>();
				ParserHelper ph = new ParserHelper();
			
				PlainSelect plainSelect = (PlainSelect) existExp.getSelectBody();

				if(plainSelect.getWhere() != null){
					WhereVisitor whereVisitor = new WhereVisitor();
					plainSelect.getWhere().accept(whereVisitor);
					if(whereVisitor.getParserExpressions().size() > 0)
						plainSelect = whereVisitor.processingOperators(plainSelect, whereVisitor);
				}

				tables = ph.extractJoinTables(plainSelect);
				
				ColumnsInvolved columnsInvolved = new ColumnsInvolved(tables);
				if(plainSelect.getWhere() != null){
					plainSelect.getWhere().accept(columnsInvolved);
					plainSelect.setWhere(columnsInvolved.getNewWhere());
				}

				ParserExpression pe = new ParserExpression();

				Table _table = columnsInvolved.getJoinTable();

				if(_table == null && anyExpression.getAnyType() != AnyType.ALL)
				{
					// Create a new SubSelect
					SubSelect subSelect = new SubSelect();

					// Set the PlainSelect as the subquery
					subSelect.setSelectBody(plainSelect);
					subSelect.setAlias(new Alias("C" + count));
					
					GroupByElement newGroupByElement = new GroupByElement();
					for(SelectItem si : plainSelect.getSelectItems()){
						SelectExpressionItem sei = (SelectExpressionItem) si;
						newGroupByElement.addGroupByExpressions(sei.getExpression());
					}
					
					plainSelect.setGroupByElement(newGroupByElement);

					pe.setSelect(subSelect);
					pe.setJoinTable(_columns.get(0).getTable());
					pe.setJoinExpressions(_newJoin);
				}else{
					PlainSelect _outerJoin = new PlainSelect();
					_table = _table == null ? _columns.get(0).getTable() : _table;
					_outerJoin.setFromItem(_table);
					Join newJoin = new Join();
					
					List<SelectItem> selectItems = new ArrayList<>();
					for(Column c : columnsInvolved.getCorrelatedColums())
						selectItems.add(new SelectExpressionItem(c));

					for(Column c : _columns)
						selectItems.add(new SelectExpressionItem(c));

					_outerJoin.setSelectItems(selectItems);

					// Create a new SubSelect
					SubSelect subSelect = new SubSelect();
					
					selectItems = plainSelect.getSelectItems();
					for(SelectItem item : selectItems){
						SelectExpressionItem sei = (SelectExpressionItem) item;
						if(sei.getExpression() instanceof Column){
							Column _c = (Column) sei.getExpression();
							for(Column c : columnsInvolved.getColumns())
								if(!ph.areColumnsEquals(c, _c))
									selectItems.add(new SelectExpressionItem(c));
						}
					}
					
					plainSelect.setSelectItems(selectItems);
					
					// Set the PlainSelect as the subquery
					subSelect.setSelectBody(plainSelect);
					subSelect.setAlias(new Alias("C" + count));
					newJoin.setRightItem(subSelect);
					newJoin.setLeft(false);
					newJoin.setOuter(false);
						
					
					List<Expression> _joinExp = columnsInvolved.getExpressions();
					_joinExp.add(_newJoin);

					List<Expression> joinExp = new ArrayList<>();
					joinExp.add(ph.buildAndExpression(_joinExp));
					newJoin.setOnExpressions(joinExp);
					_outerJoin.addJoins(newJoin);

					GroupByElement newGroupByElement = new GroupByElement();
					newGroupByElement.addGroupByExpressions(columnsInvolved.getCorrelatedColums());
					_outerJoin.setGroupByElement(newGroupByElement);

					SubSelect _subSelect = new SubSelect();
					_subSelect.setSelectBody(_outerJoin);
					_subSelect.setAlias(new Alias("nestedT" + count));
					
					pe.setSelect(_subSelect);
					pe.setJoinTable(_table);

					pe.setJoinExpressions(ph.equiJoins(_joinExp, _table,"nestedT" + count));
				}


				parserExpressions.add(pe);

				count++;
				
			}
		}

		@Override
		public void visit(SubSelect subSelect){
			if(subSelect.getSelectBody() instanceof PlainSelect){
				List<Table> tables = new ArrayList<>();
				ParserHelper ph = new ParserHelper();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();

				if(plainSelect.getWhere() != null){
					WhereVisitor whereVisitor = new WhereVisitor();
					plainSelect.getWhere().accept(whereVisitor);
					if(whereVisitor.getParserExpressions().size() > 0)
						plainSelect = whereVisitor.processingOperators(plainSelect, whereVisitor);
				}

				tables = ph.extractJoinTables(plainSelect);
				
				ColumnsInvolved columnsInvolved = new ColumnsInvolved(tables);
				if(plainSelect.getWhere() != null)
					plainSelect.getWhere().accept(columnsInvolved);
					plainSelect.setWhere(columnsInvolved.getNewWhere());

				ParserExpression pe = new ParserExpression();

				for(Column c : columnsInvolved.getColumns())
					plainSelect.addSelectItems(new SelectExpressionItem(c));

				// Create a new SubSelect
				SubSelect newSubSelect = new SubSelect();

				// Set the PlainSelect as the subquery
				newSubSelect.setSelectBody(plainSelect);
				newSubSelect.setAlias(new Alias("C" + count));
				
				boolean hasGroupByEle = false;
				GroupByElement newGroupByElement = new GroupByElement();
				for(SelectItem si : plainSelect.getSelectItems()){
					SelectExpressionItem sei = (SelectExpressionItem) si;
					if(sei.getExpression() instanceof Column){
						hasGroupByEle = true;
						newGroupByElement.addGroupByExpressions(sei.getExpression());
					}
				}
				
				if(hasGroupByEle)
					plainSelect.setGroupByElement(newGroupByElement);

				List<Expression> joinExp = new ArrayList<>();
				
				if(columnsInvolved.getExpressions() != null) 
					joinExp.addAll(columnsInvolved.getExpressions());
				joinExp.add(_newJoin);

				pe.setSelect(newSubSelect);
				pe.setJoinTable(null);
				pe.setWhereExpressions(joinExp);

				parserExpressions.add(pe);

				count++;
			}		
		}

		@Override
		public void visit(AndExpression andExpression) {
			isInAndExpression = true;

			super.visit(andExpression);
		}

		@Override
		public void visit(OrExpression orExpression) {
			isInOrExpression = true;

			super.visit(orExpression);

		}

		@Override
		public void visit(IsNullExpression isNullExpression){
			if(newWhere == null)
				newWhere = isNullExpression;
			else 
				if (isInAndExpression)
					newWhere = new AndExpression(newWhere, isNullExpression);
				else if (isInOrExpression)
					newWhere = new OrExpression(newWhere, isNullExpression);

			super.visit(isNullExpression);
		}

		@Override
		public void visit(LikeExpression likeExpression){
			if(newWhere == null)
				newWhere = likeExpression;
			else 
				if (isInAndExpression)
					newWhere = new AndExpression(newWhere, likeExpression);
				else if (isInOrExpression)
					newWhere = new OrExpression(newWhere, likeExpression);

			super.visit(likeExpression);
		}

		@Override
		public void visit(GreaterThan greaterThan){
			 if(greaterThan.getRightExpression() instanceof AnyComparisonExpression || greaterThan.getLeftExpression() instanceof AnyComparisonExpression){

				SubSelect subSelect = (SubSelect) ((AnyComparisonExpression) greaterThan.getRightExpression()).getSubSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
				Column column = null;

				if(selectExpressionItem.getExpression() instanceof Column)
						column = (Column) selectExpressionItem.getExpression();
				else{
					if(selectExpressionItem.getAlias() == null){
						selectExpressionItem.setAlias(new Alias("_col1"));
						column = new Column(new Table("C"+count), "_col1");
					}else
						column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
				}
				
				if(greaterThan.getRightExpression() instanceof AnyComparisonExpression){
					_newJoin = new GreaterThan();
					Column column2 = (Column) greaterThan.getLeftExpression();
					
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(greaterThan.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));
					
					if(((AnyComparisonExpression) greaterThan.getRightExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = greaterThan;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, greaterThan);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, greaterThan);
					}
						
					
				}else if(greaterThan.getLeftExpression() instanceof AnyComparisonExpression){
					_newJoin = new GreaterThan();
					Column column2 = (Column) greaterThan.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(greaterThan.getRightExpression());

					if(((AnyComparisonExpression) greaterThan.getLeftExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = greaterThan;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, greaterThan);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, greaterThan);
					}
				}

				
			}else if(greaterThan.getRightExpression() instanceof SubSelect || greaterThan.getLeftExpression() instanceof SubSelect){
				PlainSelect plainSelect = null;
				if(greaterThan.getRightExpression() instanceof SubSelect){
					plainSelect = (PlainSelect) ((SubSelect) greaterThan.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}

					_newJoin = new GreaterThan();
					Column column2 = (Column) greaterThan.getLeftExpression();
					_columns.add(column2);
					
					((BinaryExpression) _newJoin).setLeftExpression(greaterThan.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));	
				}
				else{
					plainSelect = (PlainSelect) ((SubSelect) greaterThan.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}
					_newJoin = new GreaterThan();
					Column column2 = (Column) greaterThan.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(greaterThan.getRightExpression());
				}

			}else{
					if(newWhere == null)
						newWhere = greaterThan;
					else 
						if (isInAndExpression)
							newWhere = new AndExpression(newWhere, greaterThan);
						else if (isInOrExpression)
							newWhere = new OrExpression(newWhere, greaterThan);
			}
			super.visit(greaterThan);	
		}

		@Override
		public void visit(GreaterThanEquals greaterThan){
			 if(greaterThan.getRightExpression() instanceof AnyComparisonExpression || greaterThan.getLeftExpression() instanceof AnyComparisonExpression){

				SubSelect subSelect = (SubSelect) ((AnyComparisonExpression) greaterThan.getRightExpression()).getSubSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
				Column column = null;

				if(selectExpressionItem.getExpression() instanceof Column)
						column = (Column) selectExpressionItem.getExpression();
				else{
					if(selectExpressionItem.getAlias() == null){
						selectExpressionItem.setAlias(new Alias("_col1"));
						column = new Column(new Table("C"+count), "_col1");
					}else
						column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
				}
				
				if(greaterThan.getRightExpression() instanceof AnyComparisonExpression){
					_newJoin = new GreaterThanEquals();
					Column column2 = (Column) greaterThan.getLeftExpression();
					
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(greaterThan.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));
					
					if(((AnyComparisonExpression) greaterThan.getRightExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = greaterThan;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, greaterThan);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, greaterThan);
					}
						
					
				}else if(greaterThan.getLeftExpression() instanceof AnyComparisonExpression){
					_newJoin = new GreaterThanEquals();
					Column column2 = (Column) greaterThan.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(greaterThan.getRightExpression());

					if(((AnyComparisonExpression) greaterThan.getLeftExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = greaterThan;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, greaterThan);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, greaterThan);
					}
				}

				
			}else if(greaterThan.getRightExpression() instanceof SubSelect || greaterThan.getLeftExpression() instanceof SubSelect){
				PlainSelect plainSelect = null;
				if(greaterThan.getRightExpression() instanceof SubSelect){
					plainSelect = (PlainSelect) ((SubSelect) greaterThan.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}

					_newJoin = new GreaterThanEquals();
					Column column2 = (Column) greaterThan.getLeftExpression();
					_columns.add(column2);
					
					((BinaryExpression) _newJoin).setLeftExpression(greaterThan.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));	
				}
				else{
					plainSelect = (PlainSelect) ((SubSelect) greaterThan.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}
					_newJoin = new GreaterThanEquals();
					Column column2 = (Column) greaterThan.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(greaterThan.getRightExpression());
				}

			}else{
					if(newWhere == null)
						newWhere = greaterThan;
					else 
						if (isInAndExpression)
							newWhere = new AndExpression(newWhere, greaterThan);
						else if (isInOrExpression)
							newWhere = new OrExpression(newWhere, greaterThan);
			}
			super.visit(greaterThan);	
		}

		@Override
		public void visit(MinorThan minorThan){
			 if(minorThan.getRightExpression() instanceof AnyComparisonExpression || minorThan.getLeftExpression() instanceof AnyComparisonExpression){

				SubSelect subSelect = (SubSelect) ((AnyComparisonExpression) minorThan.getRightExpression()).getSubSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
				Column column = null;

				if(selectExpressionItem.getExpression() instanceof Column)
						column = (Column) selectExpressionItem.getExpression();
				else{
					if(selectExpressionItem.getAlias() == null){
						selectExpressionItem.setAlias(new Alias("_col1"));
						column = new Column(new Table("C"+count), "_col1");
					}else
						column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
				}
				
				if(minorThan.getRightExpression() instanceof AnyComparisonExpression){
					_newJoin = new MinorThan();
					Column column2 = (Column) minorThan.getLeftExpression();
					
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(minorThan.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));
					
					if(((AnyComparisonExpression) minorThan.getRightExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = minorThan;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, minorThan);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, minorThan);
					}
						
					
				}else if(minorThan.getLeftExpression() instanceof AnyComparisonExpression){
					_newJoin = new MinorThan();
					Column column2 = (Column) minorThan.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(minorThan.getRightExpression());

					if(((AnyComparisonExpression) minorThan.getLeftExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = minorThan;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, minorThan);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, minorThan);
					}
				}

				
			}else if(minorThan.getRightExpression() instanceof SubSelect || minorThan.getLeftExpression() instanceof SubSelect){
				PlainSelect plainSelect = null;
				if(minorThan.getRightExpression() instanceof SubSelect){
					plainSelect = (PlainSelect) ((SubSelect) minorThan.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}

					_newJoin = new MinorThan();
					Column column2 = (Column) minorThan.getLeftExpression();
					_columns.add(column2);
					
					((BinaryExpression) _newJoin).setLeftExpression(minorThan.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));	
				}
				else{
					plainSelect = (PlainSelect) ((SubSelect) minorThan.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}
					_newJoin = new MinorThan();
					Column column2 = (Column) minorThan.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(minorThan.getRightExpression());
				}

			}else{
					if(newWhere == null)
						newWhere = minorThan;
					else 
						if (isInAndExpression)
							newWhere = new AndExpression(newWhere, minorThan);
						else if (isInOrExpression)
							newWhere = new OrExpression(newWhere, minorThan);
			}
			super.visit(minorThan);	
		}

		@Override
		public void visit(MinorThanEquals minorThan){
			 if(minorThan.getRightExpression() instanceof AnyComparisonExpression || minorThan.getLeftExpression() instanceof AnyComparisonExpression){

				SubSelect subSelect = (SubSelect) ((AnyComparisonExpression) minorThan.getRightExpression()).getSubSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
				Column column = null;

				if(selectExpressionItem.getExpression() instanceof Column)
						column = (Column) selectExpressionItem.getExpression();
				else{
					if(selectExpressionItem.getAlias() == null){
						selectExpressionItem.setAlias(new Alias("_col1"));
						column = new Column(new Table("C"+count), "_col1");
					}else
						column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
				}
				
				if(minorThan.getRightExpression() instanceof AnyComparisonExpression){
					_newJoin = new MinorThanEquals();
					Column column2 = (Column) minorThan.getLeftExpression();
					
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(minorThan.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));
					
					if(((AnyComparisonExpression) minorThan.getRightExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = minorThan;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, minorThan);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, minorThan);
					}
						
					
				}else if(minorThan.getLeftExpression() instanceof AnyComparisonExpression){
					_newJoin = new MinorThanEquals();
					Column column2 = (Column) minorThan.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(minorThan.getRightExpression());

					if(((AnyComparisonExpression) minorThan.getLeftExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = minorThan;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, minorThan);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, minorThan);
					}
				}

				
			}else if(minorThan.getRightExpression() instanceof SubSelect || minorThan.getLeftExpression() instanceof SubSelect){
				PlainSelect plainSelect = null;
				if(minorThan.getRightExpression() instanceof SubSelect){
					plainSelect = (PlainSelect) ((SubSelect) minorThan.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}

					_newJoin = new MinorThanEquals();
					Column column2 = (Column) minorThan.getLeftExpression();
					_columns.add(column2);
					
					((BinaryExpression) _newJoin).setLeftExpression(minorThan.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));	
				}
				else{
					plainSelect = (PlainSelect) ((SubSelect) minorThan.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}
					_newJoin = new MinorThanEquals();
					Column column2 = (Column) minorThan.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(minorThan.getRightExpression());
				}

			}else{
					if(newWhere == null)
						newWhere = minorThan;
					else 
						if (isInAndExpression)
							newWhere = new AndExpression(newWhere, minorThan);
						else if (isInOrExpression)
							newWhere = new OrExpression(newWhere, minorThan);
			}
			super.visit(minorThan);	
		}

		@Override
		public void visit(EqualsTo equalsTo){
			 if(equalsTo.getRightExpression() instanceof AnyComparisonExpression || equalsTo.getLeftExpression() instanceof AnyComparisonExpression){

				SubSelect subSelect = (SubSelect) ((AnyComparisonExpression) equalsTo.getRightExpression()).getSubSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
				Column column = null;

				if(selectExpressionItem.getExpression() instanceof Column)
						column = (Column) selectExpressionItem.getExpression();
				else{
					if(selectExpressionItem.getAlias() == null){
						selectExpressionItem.setAlias(new Alias("_col1"));
						column = new Column(new Table("C"+count), "_col1");
					}else
						column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
				}
				
				if(equalsTo.getRightExpression() instanceof AnyComparisonExpression){
					_newJoin = new EqualsTo();
					Column column2 = (Column) equalsTo.getLeftExpression();
					
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(equalsTo.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));
					
					if(((AnyComparisonExpression) equalsTo.getRightExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = equalsTo;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, equalsTo);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, equalsTo);
					}
						
					
				}else if(equalsTo.getLeftExpression() instanceof AnyComparisonExpression){
					_newJoin = new EqualsTo();
					Column column2 = (Column) equalsTo.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(equalsTo.getRightExpression());

					if(((AnyComparisonExpression) equalsTo.getLeftExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = equalsTo;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, equalsTo);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, equalsTo);
					}
				}

				
			}else if(equalsTo.getRightExpression() instanceof SubSelect || equalsTo.getLeftExpression() instanceof SubSelect){
				PlainSelect plainSelect = null;
				if(equalsTo.getRightExpression() instanceof SubSelect){
					plainSelect = (PlainSelect) ((SubSelect) equalsTo.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}

					_newJoin = new EqualsTo();
					Column column2 = (Column) equalsTo.getLeftExpression();
					_columns.add(column2);
					
					((BinaryExpression) _newJoin).setLeftExpression(equalsTo.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));	
				}
				else{
					plainSelect = (PlainSelect) ((SubSelect) equalsTo.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}
					_newJoin = new EqualsTo();
					Column column2 = (Column) equalsTo.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(equalsTo.getRightExpression());
				}

			}else{
					if(newWhere == null)
						newWhere = equalsTo;
					else 
						if (isInAndExpression)
							newWhere = new AndExpression(newWhere, equalsTo);
						else if (isInOrExpression)
							newWhere = new OrExpression(newWhere, equalsTo);
			}
			super.visit(equalsTo);	
		}

		@Override
		public void visit(NotEqualsTo equalsTo){
			 if(equalsTo.getRightExpression() instanceof AnyComparisonExpression || equalsTo.getLeftExpression() instanceof AnyComparisonExpression){

				SubSelect subSelect = (SubSelect) ((AnyComparisonExpression) equalsTo.getRightExpression()).getSubSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
				Column column = null;

				if(selectExpressionItem.getExpression() instanceof Column)
						column = (Column) selectExpressionItem.getExpression();
				else{
					if(selectExpressionItem.getAlias() == null){
						selectExpressionItem.setAlias(new Alias("_col1"));
						column = new Column(new Table("C"+count), "_col1");
					}else
						column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
				}
				
				if(equalsTo.getRightExpression() instanceof AnyComparisonExpression){
					_newJoin = new NotEqualsTo();
					Column column2 = (Column) equalsTo.getLeftExpression();
					
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(equalsTo.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));
					
					if(((AnyComparisonExpression) equalsTo.getRightExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = equalsTo;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, equalsTo);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, equalsTo);
					}
						
					
				}else if(equalsTo.getLeftExpression() instanceof AnyComparisonExpression){
					_newJoin = new NotEqualsTo();
					Column column2 = (Column) equalsTo.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(equalsTo.getRightExpression());

					if(((AnyComparisonExpression) equalsTo.getLeftExpression()).getAnyType() == AnyType.ALL){
						if(newWhere == null)
							newWhere = equalsTo;
						else 
							if (isInAndExpression)
								newWhere = new AndExpression(newWhere, equalsTo);
							else if (isInOrExpression)
								newWhere = new OrExpression(newWhere, equalsTo);
					}
				}

				
			}else if(equalsTo.getRightExpression() instanceof SubSelect || equalsTo.getLeftExpression() instanceof SubSelect){
				PlainSelect plainSelect = null;
				if(equalsTo.getRightExpression() instanceof SubSelect){
					plainSelect = (PlainSelect) ((SubSelect) equalsTo.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}

					_newJoin = new NotEqualsTo();
					Column column2 = (Column) equalsTo.getLeftExpression();
					_columns.add(column2);
					
					((BinaryExpression) _newJoin).setLeftExpression(equalsTo.getLeftExpression());
					((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), column.getColumnName()));	
				}
				else{
					plainSelect = (PlainSelect) ((SubSelect) equalsTo.getRightExpression()).getSelectBody();
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) plainSelect.getSelectItems().get(0);
					Column column = null;

					if(selectExpressionItem.getExpression() instanceof Column)
						 column = (Column) selectExpressionItem.getExpression();
					else{
						if(selectExpressionItem.getAlias() == null){
							selectExpressionItem.setAlias(new Alias("_col1"));
							column = new Column(new Table("C"+count), "_col1");
						}else
							column = new Column(new Table("C"+count), selectExpressionItem.getAlias().getName());
					}
					_newJoin = new NotEqualsTo();
					Column column2 = (Column) equalsTo.getRightExpression();
					_columns.add(column2);
					((BinaryExpression) _newJoin).setLeftExpression(new Column(new Table("C"+count), column.getColumnName()));
					((BinaryExpression) _newJoin).setRightExpression(equalsTo.getRightExpression());
				}

			}else{
					if(newWhere == null)
						newWhere = equalsTo;
					else 
						if (isInAndExpression)
							newWhere = new AndExpression(newWhere, equalsTo);
						else if (isInOrExpression)
							newWhere = new OrExpression(newWhere, equalsTo);
			}
			super.visit(equalsTo);	
		}
		

		public List<ParserExpression> getParserExpressions() {
			return parserExpressions;
		}

		public Expression getNewWhere() {
			return newWhere;
		}
	}


	private static class ColumnsInvolved extends ExpressionVisitorAdapter {
		private List<Table> tables;
		private List<Column> columns;
		private List<Column> correlatedColums;
		private List<Expression> expressions;
		private Expression newWhere = null;
		private Table tableJoin;
		private boolean isInAndExpression;
		private boolean isInOrExpression;

		public ColumnsInvolved(List<Table> tables){
			this.tables = tables;
			columns = new ArrayList<>();
			correlatedColums = new ArrayList<>();
			expressions = new ArrayList<>();
		}

		private boolean verifyOuterTable(Column column){
			ParserHelper ph = new ParserHelper();
			boolean isOuterTable = true;
			for(Table table : tables){				
				if(ph.areTablesEqual(table, column.getTable()))
					isOuterTable = false;
			}
			return isOuterTable;
		}

		private void createExpression(Expression exp1, Expression exp2, Object comparison){
			Column col1 = (Column) exp1;
			Column col2 = (Column) exp2;

			columns.add(new Column().withColumnName(col2.getColumnName()).withTable(new Table(col2.getTable().getFullyQualifiedName())));
			correlatedColums.add(new Column().withColumnName(col1.getColumnName()).withTable(col1.getTable()));
			col2.setTable(new Table("C"+count));

			tableJoin = col1.getTable();
			Expression exp = null;
			if(comparison instanceof EqualsTo)
				exp = new EqualsTo().withLeftExpression(exp1).withRightExpression(exp2);
			else if(comparison instanceof NotEqualsTo)
				exp = new NotEqualsTo().withLeftExpression(exp1).withRightExpression(exp2);
			else if(comparison instanceof GreaterThan)
				exp = new GreaterThan().withLeftExpression(exp1).withRightExpression(exp2);
			else if(comparison instanceof GreaterThanEquals)
				exp = new GreaterThanEquals().withLeftExpression(exp1).withRightExpression(exp2);
			else if(comparison instanceof MinorThan)
				exp = new MinorThan().withLeftExpression(exp1).withRightExpression(exp2);
			else if(comparison instanceof MinorThanEquals)
				exp = new MinorThanEquals().withLeftExpression(exp1).withRightExpression(exp2);

			expressions.add(exp);
		}


		@Override
		public void visit(AndExpression andExpression) {
			isInAndExpression = true;

			super.visit(andExpression);
		}

		@Override
		public void visit(OrExpression orExpression) {
			isInOrExpression = true;

			super.visit(orExpression);

		}

		@Override
		public void visit(EqualsTo equalsTo) {
			boolean outerTable = false;
			if(equalsTo.getLeftExpression() instanceof Column && equalsTo.getRightExpression() instanceof Column){
				Column left = (Column) equalsTo.getLeftExpression();
				Column right = (Column) equalsTo.getRightExpression();
				
				if(verifyOuterTable(left))
				{
					outerTable = true;
					createExpression(equalsTo.getLeftExpression(), equalsTo.getRightExpression(), equalsTo);

				}
				else if(verifyOuterTable(right))
				{
					outerTable = true;
					createExpression(equalsTo.getRightExpression(), equalsTo.getLeftExpression(),equalsTo);

				}
			}	
			
			if (!outerTable) {
				if(newWhere == null)
					newWhere = equalsTo;
				else 
					if (isInAndExpression)
						newWhere = new AndExpression(newWhere, equalsTo);
					else if (isInOrExpression)
						newWhere = new OrExpression(newWhere, equalsTo);
			}
			
			super.visit(equalsTo);
		}

		@Override
		public void visit(NotEqualsTo NoEqualsTo) {
			boolean outerTable = false;
			if(NoEqualsTo.getLeftExpression() instanceof Column && NoEqualsTo.getRightExpression() instanceof Column){
				Column left = (Column) NoEqualsTo.getLeftExpression();
				Column right = (Column) NoEqualsTo.getRightExpression();
				
				if(verifyOuterTable(left))
				{
					outerTable = true;
					createExpression(NoEqualsTo.getLeftExpression(), NoEqualsTo.getRightExpression(), NoEqualsTo);

				}
				else if(verifyOuterTable(right))
				{
					outerTable = true;
					createExpression(NoEqualsTo.getRightExpression(), NoEqualsTo.getLeftExpression(), NoEqualsTo);

				}				
			}

			if (!outerTable) {
				if(newWhere == null)
					newWhere = NoEqualsTo;
				else 
					if (isInAndExpression)
						newWhere = new AndExpression(newWhere, NoEqualsTo);
					else if (isInOrExpression)
						newWhere = new OrExpression(newWhere, NoEqualsTo);
			}

			super.visit(NoEqualsTo);
		}

		@Override
		public void visit(GreaterThan greaterThan){
			boolean outerTable = false;
			if(greaterThan.getLeftExpression() instanceof Column && greaterThan.getRightExpression() instanceof Column){
				Column left = (Column) greaterThan.getLeftExpression();
				Column right = (Column) greaterThan.getRightExpression();
				
				if(verifyOuterTable(left))
				{
					outerTable = true;
					createExpression(greaterThan.getLeftExpression(), greaterThan.getRightExpression(), greaterThan);
				}
				else if(verifyOuterTable(right))
				{
					outerTable = true;
					createExpression(greaterThan.getRightExpression(), greaterThan.getLeftExpression(), greaterThan);
				}				
			}

			if (!outerTable) {
				if(newWhere == null)
					newWhere = greaterThan;
				else 
					if (isInAndExpression)
						newWhere = new AndExpression(newWhere, greaterThan);
					else if (isInOrExpression)
						newWhere = new OrExpression(newWhere, greaterThan);
			}

			super.visit(greaterThan);	
		}

		@Override
		public void visit(GreaterThanEquals greaterThan){
			boolean outerTable = false;
			if(greaterThan.getLeftExpression() instanceof Column && greaterThan.getRightExpression() instanceof Column){
				Column left = (Column) greaterThan.getLeftExpression();
				Column right = (Column) greaterThan.getRightExpression();
				
				if(verifyOuterTable(left))
				{
					outerTable = true;
					createExpression(greaterThan.getLeftExpression(), greaterThan.getRightExpression(), greaterThan);
				}
				else if(verifyOuterTable(right))
				{
					outerTable = true;
					createExpression(greaterThan.getRightExpression(), greaterThan.getLeftExpression(), greaterThan);
				}				
			}
			if(!outerTable){
				if(newWhere == null)
					newWhere = greaterThan;
				else 
					if (isInAndExpression)
						newWhere = new AndExpression(newWhere, greaterThan);
					else if (isInOrExpression)
						newWhere = new OrExpression(newWhere, greaterThan);
			}
			super.visit(greaterThan);	
		}

		@Override
		public void visit(MinorThan minorThan){
			boolean outerTable = false;
			if(minorThan.getLeftExpression() instanceof Column && minorThan.getRightExpression() instanceof Column){
				Column left = (Column) minorThan.getLeftExpression();
				Column right = (Column) minorThan.getRightExpression();
				
				if(verifyOuterTable(left))
				{
					outerTable = true;
					createExpression(minorThan.getLeftExpression(), minorThan.getRightExpression(), minorThan);
				}
				else if(verifyOuterTable(right))
				{
					outerTable = true;
					createExpression(minorThan.getRightExpression(), minorThan.getLeftExpression(), minorThan);
				}				
			}
			if (!outerTable) {
				if(newWhere == null)
					newWhere = minorThan;
				else 
					if (isInAndExpression)
						newWhere = new AndExpression(newWhere, minorThan);
					else if (isInOrExpression)
						newWhere = new OrExpression(newWhere, minorThan);
			}
			super.visit(minorThan);	
		}

		@Override
		public void visit(MinorThanEquals minorThan){
			boolean outerTable = false;
			if(minorThan.getLeftExpression() instanceof Column && minorThan.getRightExpression() instanceof Column){
				Column left = (Column) minorThan.getLeftExpression();
				Column right = (Column) minorThan.getRightExpression();
				
				if(verifyOuterTable(left))
				{
					outerTable = true;
					createExpression(minorThan.getLeftExpression(), minorThan.getRightExpression(), minorThan);
				}
				else if(verifyOuterTable(right))
				{
					outerTable = true;
					createExpression(minorThan.getRightExpression(), minorThan.getLeftExpression(), minorThan);
				}				
			}
			if (!outerTable) {
				if(newWhere == null)
					newWhere = minorThan;
				else 
					if (isInAndExpression)
						newWhere = new AndExpression(newWhere, minorThan);
					else if (isInOrExpression)
						newWhere = new OrExpression(newWhere, minorThan);
			}
			super.visit(minorThan);	
		}

		@Override
		public void visit(IsNullExpression isNullExpression){
			if(newWhere == null)
				newWhere = isNullExpression;
			else 
				if (isInAndExpression)
					newWhere = new AndExpression(newWhere, isNullExpression);
				else if (isInOrExpression)
					newWhere = new OrExpression(newWhere, isNullExpression);
			super.visit(isNullExpression);
		}

		@Override
		public void visit(LikeExpression likeExpression){
			if(newWhere == null)
				newWhere = likeExpression;
			else 
				if (isInAndExpression)
					newWhere = new AndExpression(newWhere, likeExpression);
				else if (isInOrExpression)
					newWhere = new OrExpression(newWhere, likeExpression);

			super.visit(likeExpression);
		}

		public Expression getNewWhere() {
			return newWhere;
		}

		public List<Expression> getExpressions() {
			return expressions;
		}

		public List<Column> getCorrelatedColums() {
			return correlatedColums;
		}

		public List<Column> getColumns() {
			return columns;
		}

		public Table getJoinTable() {
			return tableJoin;
		}
	}

	private static class FromTables extends FromItemVisitorAdapter {
		List<Table> tables = new ArrayList<>();

		@Override
		public void visit(SubSelect subSelect) {
			tables.add(new Table(subSelect.getAlias().getName()));
		}

		@Override
		public void visit(Table table) {
			tables.add(table);
		}
	}

	public static class ExistsVisitor extends ExpressionVisitorAdapter {
        private boolean hasExistsClause = false;
		private boolean hasInClause = false;
		private boolean hasAndClause = false;
		private boolean hasSubSelect = false;

		private List<SubSelect> exists;
		private List<Expression> expressions;
		private Expression inWhereExp = null;
		private List<ParserExpression> parserExpressions;
		private List<Table> tables;
		private List<String> inTables;
		private List<SubSelect> inselects;
		private List<Expression> inExpressions;
		private boolean _hasInClause = false;
		private boolean _hasExistsClause = false;
		private boolean _hasSubSelect = false;


		public List<ParserExpression> getParserExpressions() {
			return parserExpressions;
		}

		public ExistsVisitor(List<Table> tables){
			exists = new ArrayList<>();
			expressions = new ArrayList<>();
			inExpressions = new ArrayList<>();
			inselects = new ArrayList<>();
			parserExpressions = new ArrayList<>();
			this.tables = tables;
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

				//AndExpression.setRightExpression(expressions.get(0));
				AndExpression.setRightExpression(new EqualsTo(new LongValue(1), new LongValue(1)));
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

				//remove
				//tables = fromVisitor.tables;
				
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

			ParserHelper ph = new ParserHelper();
			

			if(existExp.getSelectBody() instanceof PlainSelect){
				PlainSelect plainSelect = (PlainSelect) existExp.getSelectBody();

				//TODO falta ver quando o dentro do subquery existem clausulas do WHERE sem SUBSELECT pois são precisas passar para fora

				if(plainSelect.getWhere() != null){
					//TODO: Confrimar se este novo argumento funciona (ph.extract..)
					ExistsVisitor existsVisitor = new ExistsVisitor(ph.extractJoinTables(plainSelect));
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
					notNullExp.setNot(false);
					inWhereExp = notNullExp;
				}
				existExp.setAlias(new Alias("CI" + inCount));
				inselects.add(existExp);
				inCount++;

			}
		}

		@Override
        public void visit(NotExpression notExistsExpression) {
			if(notExistsExpression.getExpression() instanceof ExistsExpression){
				hasExistsClause = true;
				_hasExistsClause = true;

				ExistsExpression existsExpression = (ExistsExpression) notExistsExpression.getExpression();
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
					//expressions.addAll(whereVisitor.getExpressions());

					SelectItem firstSelectItem = plainSelect.getSelectItems().get(0);
					ParserExpression pe = new ParserExpression();
					if (firstSelectItem instanceof AllColumns) {
						List<SelectItem> selectItems = new ArrayList<>();
						for(Column c : whereVisitor.getColumns()){
							selectItems.add(new SelectExpressionItem(c));
							IsNullExpression notNullExp = new IsNullExpression();
							Column colWhere = new Column();
							colWhere.setColumnName(c.getColumnName());
							colWhere.setTable(new Table("C" + count));
							notNullExp.setLeftExpression(colWhere);
							notNullExp.setNot(false);
							pe.addWhereExpression(notNullExp);
						}
						plainSelect.setSelectItems(selectItems);
					}

					existExp.setAlias(new Alias("C" + count));
					
					pe.setSelect(existExp);
					pe.setJoinTable(whereVisitor.getJoinTable());
					pe.setJoinExpressions(whereVisitor.getExpressions());
					parserExpressions.add(pe);
					count++;
				}
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

				SelectItem firstSelectItem = plainSelect.getSelectItems().get(0);
			
				if (firstSelectItem instanceof AllColumns) {
					List<SelectItem> selectItems = new ArrayList<>();
					for(Column c : whereVisitor.getColumns())
						selectItems.add(new SelectExpressionItem(c));
					plainSelect.setSelectItems(selectItems);
				}
				ParserExpression pe = new ParserExpression();
				//if(whereVisitor.getExpressions().size() > 1){
					PlainSelect _outerJoin = new PlainSelect();

					ParserHelper ph = new ParserHelper();
					Table _table = new Table();

					for(Table t : this.tables){

						if(ph.areTablesEqual(t, whereVisitor.getJoinTable()))
							_table = t;
					}

					_outerJoin.setFromItem(_table);
					Join newJoin = new Join();
					
					List<SelectItem> selectItems = new ArrayList<>();
					for(Column c : whereVisitor.getCorrelatedColums())
						selectItems.add(new SelectExpressionItem(c));
					_outerJoin.setSelectItems(selectItems);

					// Create a new SubSelect
					SubSelect subSelect = new SubSelect();

					// Set the PlainSelect as the subquery
					subSelect.setSelectBody(plainSelect);
					subSelect.setAlias(new Alias("C" + count));
					newJoin.setRightItem (subSelect);
					newJoin.setLeft(false);
					newJoin.setOuter(false);
					
					List<Expression> joinExp = new ArrayList<>();
					
					joinExp.add(ph.buildAndExpression(whereVisitor.getExpressions()));
					newJoin.setOnExpressions(joinExp);
					_outerJoin.addJoins(newJoin);

					GroupByElement newGroupByElement = new GroupByElement();
					newGroupByElement.addGroupByExpressions(whereVisitor.getCorrelatedColums());
					_outerJoin.setGroupByElement(newGroupByElement);

					SubSelect _subSelect = new SubSelect();
					_subSelect.setSelectBody(_outerJoin);
					_subSelect.setAlias(new Alias("Correl" + count));
					
					pe.setSelect(_subSelect);
					pe.setJoinTable(whereVisitor.getJoinTable());
					pe.setJoinExpressions(ph.changeToEquals(whereVisitor.getExpressions(), _table,"Correl" + count));
				/* }else{
					GroupByElement newGroupByElement = new GroupByElement();
					newGroupByElement.addGroupByExpressions(whereVisitor.getColumns());
					plainSelect.setGroupByElement(newGroupByElement);

					existExp.setAlias(new Alias("C" + count));
					pe.setSelect(existExp);
					pe.setJoinTable(whereVisitor.getJoinTable());
					pe.setJoinExpressions(whereVisitor.getExpressions());
					
				}*/

				parserExpressions.add(pe);

				count++;
			}					

			//existsExpression = null;
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

	

	private static class ExistsWhereExp extends ExpressionVisitorAdapter{
		private List<Expression> expressions;
		private List<Table> tables;
		private List<Column> columns;
		private List<Column> correlatedColums;
		private Table tableJoin;
		private Boolean alias;

		public ExistsWhereExp(List<Table> tables, Boolean alias){
			this.tables = tables;
			expressions = new ArrayList<>();
			this.alias = alias;
			this.columns = new ArrayList<>();
			correlatedColums  = new ArrayList<>();
			tableJoin = null;
		}

		public List<Expression> getExpressions() {
			return expressions;
		}

		private boolean verifyOuterTable(Column column){
			boolean isOuterTable = true;
			for(Table table : tables){				
				String _alias = table.getAlias() != null ? table.getAlias().getName() : "";
				if(table.getFullyQualifiedName().equals(column.getTable().getFullyQualifiedName()) || 
				_alias.equals(column.getTable().getFullyQualifiedName())){
					isOuterTable = false;
				}
			}
			return isOuterTable;
		}

		@Override
		public void visit(EqualsTo equalsTo) {

			if(equalsTo.getLeftExpression() instanceof Column && equalsTo.getRightExpression() instanceof Column){
				Column left = (Column) equalsTo.getLeftExpression();
				Column right = (Column) equalsTo.getRightExpression();
				
				if(verifyOuterTable(left))
				{
					columns.add(new Column().withColumnName(right.getColumnName()).withTable(new Table(right.getTable().getFullyQualifiedName())));
					correlatedColums.add(new Column().withColumnName(left.getColumnName()).withTable(left.getTable()));
					if(alias) right.setTable(new Table("C"+count));

					Expression exp = new EqualsTo().withLeftExpression(equalsTo.getLeftExpression()).withRightExpression(equalsTo.getRightExpression());

					tableJoin = left.getTable();	
					expressions.add(exp);

					equalsTo.setLeftExpression(new LongValue(1));
					equalsTo.setRightExpression(new LongValue(1));
				}
				else if(verifyOuterTable(right))
				{

					columns.add(new Column().withColumnName(left.getColumnName()).withTable(new Table(left.getTable().getFullyQualifiedName())));
					correlatedColums.add(new Column().withColumnName(right.getColumnName()).withTable(right.getTable()));
					if(alias) left.setTable(new Table("C"+count));

					Expression exp = new EqualsTo().withLeftExpression(equalsTo.getLeftExpression()).withRightExpression(equalsTo.getRightExpression());

					tableJoin = right.getTable();	
					expressions.add(exp);

					equalsTo.setLeftExpression(new LongValue(1));
					equalsTo.setRightExpression(new LongValue(1));
				}
				
			}			
		}

		@Override
		public void visit(NotEqualsTo NoEqualsTo) {

			if(NoEqualsTo.getLeftExpression() instanceof Column && NoEqualsTo.getRightExpression() instanceof Column){
				Column left = (Column) NoEqualsTo.getLeftExpression();
				Column right = (Column) NoEqualsTo.getRightExpression();
				
				if(verifyOuterTable(left))
				{
					columns.add(new Column().withColumnName(right.getColumnName()).withTable(new Table(right.getTable().getFullyQualifiedName())));
					correlatedColums.add(new Column().withColumnName(left.getColumnName()).withTable(left.getTable()));
					if(alias) right.setTable(new Table("C"+count));

					Expression exp = new NotEqualsTo().withLeftExpression(NoEqualsTo.getLeftExpression()).withRightExpression(NoEqualsTo.getRightExpression());

					tableJoin = left.getTable();	
					expressions.add(exp);		

					NoEqualsTo.setLeftExpression(new LongValue(0));
					NoEqualsTo.setRightExpression(new LongValue(1));
				}
				else if(verifyOuterTable(right))
				{

					columns.add(new Column().withColumnName(left.getColumnName()).withTable(new Table(left.getTable().getFullyQualifiedName())));
					correlatedColums.add(new Column().withColumnName(right.getColumnName()).withTable(right.getTable()));
					if(alias) left.setTable(new Table("C"+count));

					Expression exp =new NotEqualsTo().withLeftExpression(NoEqualsTo.getLeftExpression()).withRightExpression(NoEqualsTo.getRightExpression());

					tableJoin = right.getTable();	
					expressions.add(exp);

					NoEqualsTo.setLeftExpression(new LongValue(0));
					NoEqualsTo.setRightExpression(new LongValue(1));
				}
				
			}
		}

		public List<Column> getCorrelatedColums() {
			return correlatedColums;
		}
		public List<Column> getColumns() {
			return columns;
		}

		public void setColumns(List<Column> columns) {
			this.columns = columns;
		}

		public Table getJoinTable(){return tableJoin;}
	}

	private static class WhereColumns extends ExpressionVisitorAdapter{
		private boolean isOuterTable = false;
		private List<Table> tables;
		private List<Column> columns;
		private Boolean alias;

		public WhereColumns(List<Table> tables, Boolean alias){
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

			for(Table table : tables){				
				String _alias = table.getAlias() != null ? table.getAlias().getName() : "";
				if(!table.getFullyQualifiedName().equals(column.getTable().getFullyQualifiedName()) && 
				   !_alias.equals(column.getTable().getFullyQualifiedName())){
					isOuterTable = true;
				}
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