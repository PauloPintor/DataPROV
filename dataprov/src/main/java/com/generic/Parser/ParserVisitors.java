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
		public void visit(Function function){
			if(newWhere == null)
				newWhere = function;
			else 
				if (isInAndExpression)
					newWhere = new AndExpression(newWhere, function);
				else if (isInOrExpression)
					newWhere = new OrExpression(newWhere, function);

			super.visit(function);
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

		@Override
		public void visit(Function function){
			if(newWhere == null)
				newWhere = function;
			else 
				if (isInAndExpression)
					newWhere = new AndExpression(newWhere, function);
				else if (isInOrExpression)
					newWhere = new OrExpression(newWhere, function);

			super.visit(function);
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
}