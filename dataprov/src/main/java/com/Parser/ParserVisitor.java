package com.Parser;

import java.util.ArrayList;
import java.util.List;

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
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
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
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

import com.Helper.ParserHelper;

public class ParserVisitor {
	private static int count = 0;

	public static class FunctionProjection implements SelectItemVisitor<Object> {
		private boolean hasFunction = false;
		private String aggExpression = "";
		private boolean minMax = false;
		private Column minMaxColumn = null;
		
		@Override
		public <S> Object visit(SelectItem<? extends Expression> selectItem, S context) {
			if(selectItem.getExpression() instanceof Function){
				Function function = (Function) selectItem.getExpression();

				if(function.getName().toLowerCase().equals("count")){
					hasFunction = true;
					aggExpression += "|| ' .count ' || CAST(1 as varchar)";
				}else if(function.getName().toLowerCase().equals("sum")){
					if(hasColumns(function.getParameters())){
						hasFunction = true;
						aggExpression += "|| ' .sum ' || CAST("+function.getParameters().toString()+" as varchar)";
					}
				}else if(function.getName().toLowerCase().equals("avg")){
					if(hasColumns(function.getParameters())){
						hasFunction = true;
						aggExpression += "|| ' .avg ' || CAST("+function.getParameters().toString()+" as varchar)";
					}
				}else if(function.getName().toLowerCase().equals("min") || function.getName().toLowerCase().equals("max")){
					if(hasColumns(function.getParameters())){
						ExpressionList<?> minMaxList = function.getParameters();
						minMaxList.forEach(e -> {
							if(e instanceof Column){
								minMaxColumn = (Column) e;
							}
						});

						hasFunction = true;
						minMax = true;
						aggExpression += "|| ' "+ (function.getName().toLowerCase().equals("min") ? "min" : "max") +" ' || CAST("+function.getParameters().toString()+" as varchar)";
					}
				}

			}else if(selectItem.getExpression() instanceof Multiplication || selectItem.getExpression() instanceof Division || selectItem.getExpression() instanceof Addition || selectItem.getExpression() instanceof Subtraction){
				String tempAggexp = arithmeticFuncs(selectItem.getExpression());
				if(tempAggexp != ""){
					hasFunction = true;
					aggExpression += "|| ' . ' || CAST("+tempAggexp+" as varchar)";
				}
			}
			return context;
		}

		private String arithmeticFuncs(Expression exp){
			String arithExp = "";
			boolean _hasFunction = false;
			if(exp instanceof Division){
				Division divison = (Division) exp;
				
				if(divison.getLeftExpression() instanceof Function){
					Function function = (Function) divison.getLeftExpression();
					if(function.getName().toLowerCase().equals("sum")){
						if(hasColumns(function.getParameters())){
							arithExp += function.getParameters().toString();
							_hasFunction = true;
						}
					}
				}else if(divison.getLeftExpression() instanceof Multiplication){
					arithExp += arithmeticFuncs(divison.getLeftExpression());
				}else if(divison.getLeftExpression() instanceof DoubleValue){
					arithExp += divison.getLeftExpression().toString();
				}

				if(divison.getRightExpression() instanceof Function){
					Function function = (Function) divison.getRightExpression();
					if(function.getName().toLowerCase().equals("sum")){
						if(hasColumns(function.getParameters())){
							arithExp += "/" + function.getParameters().toString();
							_hasFunction = true;
						}
					}
				}else if(divison.getRightExpression() instanceof Multiplication){
					arithExp += arithmeticFuncs(divison.getRightExpression());
				}else if(divison.getRightExpression() instanceof DoubleValue && _hasFunction){
					arithExp += "/" + divison.getRightExpression().toString();
				}
			}else if(exp instanceof Multiplication){
				Multiplication multiplication = (Multiplication) exp;
				
				if(multiplication.getLeftExpression() instanceof Function){
					Function function = (Function) multiplication.getLeftExpression();
	 				if(function.getName().toLowerCase().equals("sum") || function.getName().toLowerCase().equals("avg")){
						if(hasColumns(function.getParameters())){
							arithExp += function.getParameters().toString();
							_hasFunction = true;
						}
					}
				}else if(multiplication.getLeftExpression() instanceof DoubleValue){
					arithExp += multiplication.getLeftExpression().toString();
				}

				if(multiplication.getRightExpression() instanceof Function){
					Function function = (Function) multiplication.getRightExpression();
					if(function.getName().toLowerCase().equals("sum") || function.getName().toLowerCase().equals("avg")){
						if(hasColumns(function.getParameters())){
							arithExp += "*" + function.getParameters().toString();
							_hasFunction = true;
						}
					}
				}else if(multiplication.getRightExpression() instanceof DoubleValue && _hasFunction){
					arithExp += "*"+ multiplication.getRightExpression().toString();
				}
			}else if(exp instanceof Addition){
				Addition addition = (Addition) exp;
				
				if(addition.getLeftExpression() instanceof Function){
					Function function = (Function) addition.getLeftExpression();
					if(function.getName().toLowerCase().equals("sum") || function.getName().toLowerCase().equals("avg")){
						if(hasColumns(function.getParameters())){
							arithExp += function.getParameters().toString();
							_hasFunction = true;
						}
					}
				}else if(addition.getLeftExpression() instanceof DoubleValue){
					arithExp += addition.getLeftExpression().toString();
				}

				if(addition.getRightExpression() instanceof Function){
					Function function = (Function) addition.getRightExpression();
					if(function.getName().toLowerCase().equals("sum") || function.getName().toLowerCase().equals("avg")){
						if(hasColumns(function.getParameters())){
							arithExp += "+" + function.getParameters().toString();
							_hasFunction = true;
						}
					}
				}else if((addition.getRightExpression() instanceof DoubleValue || addition.getRightExpression() instanceof LongValue || addition.getRightExpression() instanceof Column) && _hasFunction){
					arithExp += "+"+ addition.getRightExpression().toString();
				}
			}

			return _hasFunction ? arithExp : "";
		}

		private boolean hasColumns(ExpressionList<?> parameters){
			for(Expression e : parameters)
			{
				if(e instanceof Column || e instanceof CaseExpression){
					return true;
				}else if(e instanceof Addition){
					Addition addition = (Addition) e;
					
					ExpressionList<?> expressionList = new ExpressionList<>(addition.getRightExpression(), addition.getLeftExpression());

					return hasColumns(expressionList);
				}else if(e instanceof Multiplication){
					Multiplication multiplication = (Multiplication) e;
					
					ExpressionList<?> expressionList = new ExpressionList<>(multiplication.getRightExpression(), multiplication.getLeftExpression());
					
					return hasColumns(expressionList);
				}else if(e instanceof Function){
					Function function = (Function) e;
					return hasColumns(function.getParameters());
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

	public static class WhereVisitor extends ExpressionVisitorAdapter<Object>{
		private List<ParserExpression> parserExpressions;
		private List<Column> _columns = null;
		private Expression newWhere = null;
		private Expression _newJoin = null;
		private List<Table> mainQueryTables = null;
		private boolean isInAndExpression;
		private boolean isInOrExpression;

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
			plainSelect.setWhere(null);
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
						plainSelect.setWhere(ph.buildAndExpression(pe.getWhereExpressions() == null ? pe.getJoinExpression() : pe.getWhereExpressions()));
					else
						plainSelect.setWhere(new AndExpression(whereVisitor.getNewWhere(), ph.buildAndExpression(pe.getWhereExpressions() == null ? pe.getJoinExpression() : pe.getWhereExpressions())));
					
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

					if(plainSelect.getWhere() == null && whereVisitor.getNewWhere() != null){
						plainSelect.setWhere(whereVisitor.getNewWhere());
					}
					
					
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

		@Override
		public <S> Void visit(ExistsExpression existsExpression, S context) {

			ParenthesedSelect existExp = (ParenthesedSelect) existsExpression.getRightExpression();
			if(existExp.getSelect() instanceof PlainSelect){
				List<Table> tables = new ArrayList<>();
				ParserHelper ph = new ParserHelper();
			
				PlainSelect plainSelect = (PlainSelect) existExp.getSelect();

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

				SelectItem<?> firstSelectItem = plainSelect.getSelectItems().get(0);
				if (firstSelectItem.getExpression() instanceof AllColumns) {
					List<SelectItem<?>> selectItems = new ArrayList<>();
					for(SelectItem<?> c : columnsInvolved.getColumns())
						selectItems.add(c);
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
				
				List<SelectItem<?>> selectItems = new ArrayList<>();
				for(SelectItem<?> c : columnsInvolved.getCorrelatedColums())
					selectItems.add(c);
				_outerJoin.setSelectItems(selectItems);

				// Create a new SubSelect
				ParenthesedSelect subSelect = new ParenthesedSelect();
				

				// Set the PlainSelect as the subquery
				subSelect.setSelect(plainSelect);
				subSelect.setAlias(new Alias("C" + count));
				newJoin.setRightItem (subSelect);
				newJoin.setLeft(false);
				newJoin.setOuter(false);
					
				List<Expression> joinExp = new ArrayList<>();
				
				joinExp.add(ph.buildAndExpression(columnsInvolved.getExpressions()));
				newJoin.setOnExpressions(joinExp);
				_outerJoin.addJoins(newJoin);

				ExpressionList<Column> groupByList = new ExpressionList<>();
				GroupByElement newGroupByElement = new GroupByElement();
				for(SelectItem<?> si : _outerJoin.getSelectItems()){
					if(si.getExpression() instanceof Column)
						groupByList.add((Column) si.getExpression());
					else
						groupByList.add(new Column(si.getAlias().getName()));
				}				
				newGroupByElement.setGroupByExpressions(groupByList);
				_outerJoin.setGroupByElement(newGroupByElement);

				ParenthesedSelect _subSelect = new ParenthesedSelect();
				_subSelect.setSelect(_outerJoin);
				_subSelect.setAlias(new Alias("nestedT" + count));
				
				pe.setSelect(_subSelect);
				pe.setJoinTable(columnsInvolved.getJoinTable());
				pe.setJoinExpressions(ph.equiJoins(columnsInvolved.getExpressions(), _table,"nestedT" + count));

				parserExpressions.add(pe);

				count++;
			}					
			return null;
		}

		@Override
		public <S> Void visit(NotExpression notExpression, S context) {
			if(notExpression.getExpression() instanceof ExistsExpression){

				ExistsExpression existsExpression = (ExistsExpression) notExpression.getExpression();
				ParenthesedSelect existExp = (ParenthesedSelect) existsExpression.getRightExpression();

				if(existExp.getSelect() instanceof PlainSelect){
					List<Table> tables = new ArrayList<>();
					ParserHelper ph = new ParserHelper();

					PlainSelect plainSelect = (PlainSelect) existExp.getSelect();

					if(plainSelect.getWhere() != null){
						WhereVisitor whereVisitor = new WhereVisitor();
						plainSelect.getWhere().accept(whereVisitor);
						if(whereVisitor.getParserExpressions().size() > 0)
							plainSelect = whereVisitor.processingOperators(plainSelect, whereVisitor);
					}

					tables = ph.extractJoinTables(plainSelect);
				
					ColumnsInvolved columnsInvolved = new ColumnsInvolved(tables);
					plainSelect.getWhere().accept(columnsInvolved, null);
					plainSelect.setWhere(columnsInvolved.getNewWhere());

					SelectItem<?> firstSelectItem = plainSelect.getSelectItems().get(0);
					ParserExpression pe = new ParserExpression();
					if (firstSelectItem.getExpression() instanceof AllColumns) {
						List<SelectItem<?>> selectItems = new ArrayList<>();
						for(SelectItem<?> c : columnsInvolved.getColumns()){
							selectItems.add(c);
						}
						plainSelect.setSelectItems(selectItems);
					}

					for(SelectItem<?> c : columnsInvolved.getColumns()){
						IsNullExpression notNullExp = new IsNullExpression();
						Column colWhere = new Column();
						String columnName = c.getAlias() == null ? ((Column)c.getExpression()).getColumnName() : c.getAlias().getName();
						colWhere.setColumnName(columnName);
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

			return null;
		}

		@Override
		public <S> Void visit(InExpression inExpression, S context) {
			ParenthesedSelect inExp = null;
			List<Column> inColumns = new ArrayList<>();
			
			if((inExpression.getLeftExpression() instanceof ParenthesedSelect)) {
				inExp = (ParenthesedSelect) inExpression.getLeftExpression();
				if(inExpression.getRightExpression() instanceof ParenthesedExpressionList){
					((ParenthesedExpressionList<?>) inExpression.getRightExpression()).forEach(e -> {
						if(e instanceof Column)
							inColumns.add((Column) e);
					});
				}else if (inExpression.getRightExpression() instanceof Column){
					inColumns.add((Column) inExpression.getRightExpression());
				}
					
			}else if((inExpression.getRightExpression() instanceof ParenthesedSelect)){
				inExp = (ParenthesedSelect) inExpression.getRightExpression();
				if(inExpression.getLeftExpression() instanceof ParenthesedExpressionList){
					((ParenthesedExpressionList<?>) inExpression.getLeftExpression()).forEach(e -> {
						if(e instanceof Column)
							inColumns.add((Column) e);
					});
				}else if (inExpression.getLeftExpression() instanceof Column){
					inColumns.add((Column) inExpression.getLeftExpression());
				}
			}
			
			if(inExp != null && inExp.getSelect() instanceof PlainSelect){
				List<Table> tables = new ArrayList<>();
				ParserHelper ph = new ParserHelper();
			
				PlainSelect plainSelect = (PlainSelect) inExp.getSelect();

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
					ParenthesedSelect subSelect = new ParenthesedSelect();

					// Set the PlainSelect as the subquery
					subSelect.setSelect(plainSelect);
					subSelect.setAlias(new Alias("C" + count));
					Column inColumn = new Column();
					ExpressionList<Column> groupByList = new ExpressionList<>();
					
					for(SelectItem<?> si : plainSelect.getSelectItems()){
						SelectItem<?> sei = (SelectItem<?>) si;
						inColumn.setColumnName(((Column) sei.getExpression()).getColumnName());
						inColumn.setTable(new Table("C" + count));
						
						groupByList.addExpression((Column) sei.getExpression());
						
					}

					GroupByElement newGroupByElement = new GroupByElement();
					newGroupByElement.setGroupByExpressions(groupByList);
				

					if(inExpression.isNot()){
						for(SelectItem<?> c : columnsInvolved.getColumns()){
							IsNullExpression notNullExp = new IsNullExpression();
							Column colWhere = new Column();
							String columnName = c.getAlias() == null ? ((Column)c.getExpression()).getFullyQualifiedName() : c.getAlias().getName();
							colWhere.setColumnName(columnName);
							colWhere.setTable(new Table("C" + count));
							notNullExp.setLeftExpression(colWhere);
							notNullExp.setNot(false);
							pe.addWhereExpression(notNullExp);
						}

						for(SelectItem<?> si : plainSelect.getSelectItems()){
							SelectItem<?> sei = (SelectItem<?>) si;
							Column colWhere = new Column();
							colWhere.setColumnName(((Column) sei.getExpression()).getColumnName());
							colWhere.setTable(new Table("C" + count));
							IsNullExpression notNullExp = new IsNullExpression();
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
					
					List<SelectItem<?>> selectItems = new ArrayList<>();
					for(SelectItem<?> c : columnsInvolved.getCorrelatedColums())
						selectItems.add(c);

					for(Column c : inColumns)
						selectItems.add(new SelectItem<>(c));
					_outerJoin.setSelectItems(selectItems);
					ExpressionList<Expression> _joinExp = new ExpressionList<>();

					// Create a new SubSelect
					ParenthesedSelect subSelect = new ParenthesedSelect();
					List<SelectItem<?>> tempSelectItems = new ArrayList<>();
					selectItems = plainSelect.getSelectItems();
					int i = 0;
					for(SelectItem<?> item : selectItems){
						
						SelectItem<?> sei = (SelectItem<?>) item;
						if(sei.getExpression() instanceof Column){
							Column _c = (Column) sei.getExpression();
							Column _newC = new Column(_c.getColumnName());
							_newC.setTable(new Table("C" + count));

							_joinExp.add(new EqualsTo(_newC, inColumns.get(i)));

							for(SelectItem<?> c : columnsInvolved.getColumns())
								if(!ph.areColumnsEquals(c, new SelectItem<>(_c)))
									tempSelectItems.add(c);
						}
						i++;
					}
					selectItems.addAll(tempSelectItems);
					
					plainSelect.setSelectItems(selectItems);
					
					// Set the PlainSelect as the subquery
					subSelect.setSelect(plainSelect);
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
					for(SelectItem<?> si : plainSelect.getSelectItems()){
						newGroupByElement.addGroupByExpressions(si.getExpression());
					}
					newGroupByElement.addGroupByExpressions(inColumns);
					_outerJoin.setGroupByElement(newGroupByElement);

					ParenthesedSelect _subSelect = new ParenthesedSelect();
					_subSelect.setSelect(_outerJoin);
					_subSelect.setAlias(new Alias("nestedT" + count));
					
					pe.setSelect(_subSelect);
					pe.setJoinTable(columnsInvolved.getJoinTable() == null ? inColumns.get(0).getTable(): columnsInvolved.getJoinTable());
					pe.setJoinExpressions(ph.equiJoins(_joinExp, _table,"nestedT" + count));
				}

				parserExpressions.add(pe);

				count++;
			}else{
				if(newWhere == null)
					newWhere = inExpression;
				else 
					if (isInAndExpression)
						newWhere = new AndExpression(newWhere, inExpression);
					else if (isInOrExpression)
						newWhere = new OrExpression(newWhere, inExpression);
			}

			return null;
		}

		@Override
		public <S> Void visit(AnyComparisonExpression anyExpression, S context) {
			ParenthesedSelect existExp = (ParenthesedSelect) anyExpression.getSelect();
            ParserExpression pe = new ParserExpression();

			if(existExp.getSelect() instanceof PlainSelect){
				List<Table> tables = new ArrayList<>();
				ParserHelper ph = new ParserHelper();
			
				PlainSelect plainSelect = (PlainSelect) existExp.getSelect();

				if(plainSelect.getWhere() != null){
					WhereVisitor whereVisitor = new WhereVisitor();
					plainSelect.getWhere().accept(whereVisitor);
					if(whereVisitor.getParserExpressions().size() > 0)
						plainSelect = whereVisitor.processingOperators(plainSelect, whereVisitor);
				}

				tables = ph.extractJoinTables(plainSelect);
				
				if(anyExpression.getAnyType() == AnyType.ALL){
					ColumnsInvolved columnsInvolved = new ColumnsInvolved(tables);
					columnsInvolved.setAll(true);
					if(plainSelect.getWhere() != null){
						plainSelect.getWhere().accept(columnsInvolved);
						plainSelect.setWhere(columnsInvolved.getNewWhere());
					}
					Table _table = columnsInvolved.getJoinTable();
					if(columnsInvolved.getCorrelatedColums().size() > 0){
						plainSelect.addSelectItems(columnsInvolved.getCorrelatedColums());

						GroupByElement newGroupByElement = new GroupByElement();
						for(SelectItem<?> si : plainSelect.getSelectItems()){
							newGroupByElement.addGroupByExpressions(si.getExpression());
						}
						plainSelect.setGroupByElement(newGroupByElement);

						pe.setJoinTable(_table);
						ExpressionList<Expression> _joinExp = new ExpressionList<>();
						_joinExp.add(new ParenthesedExpressionList<>(_newJoin));
						_joinExp.addAll(columnsInvolved.getExpressions());
						pe.addJoinExpression(ph.buildAndExpression(_joinExp));
					}else{
						pe.addJoinExpression(_newJoin);	
					}

					ParenthesedSelect subSelect = new ParenthesedSelect();
					subSelect.setSelect(plainSelect);
					subSelect.setAlias(new Alias("AllT" + count));

					pe.setSelect(subSelect);
					
				}else{
					ColumnsInvolved columnsInvolved = new ColumnsInvolved(tables);
					if(plainSelect.getWhere() != null){
						plainSelect.getWhere().accept(columnsInvolved);
						plainSelect.setWhere(columnsInvolved.getNewWhere());
					}
					Table _table = columnsInvolved.getJoinTable();
					if(_table == null && anyExpression.getAnyType() != AnyType.ALL)
					{
						// Create a new SubSelect
						ParenthesedSelect subSelect = new ParenthesedSelect();

						// Set the PlainSelect as the subquery
						subSelect.setSelect(plainSelect);
						subSelect.setAlias(new Alias("C" + count));
						ExpressionList<Expression> groupByList = new ExpressionList<>();
						
						GroupByElement newGroupByElement = new GroupByElement();
						for(SelectItem<?> si : plainSelect.getSelectItems()){
							groupByList.add(si.getExpression());
							
						}
						newGroupByElement.addGroupByExpressions(groupByList);
						plainSelect.setGroupByElement(newGroupByElement);

						pe.setSelect(subSelect);
						pe.setJoinTable(_columns.get(0).getTable());
						pe.setJoinExpressions(_newJoin);
					}else{
						PlainSelect _outerJoin = new PlainSelect();
						_table = _table == null ? _columns.get(0).getTable() : _table;
						_outerJoin.setFromItem(_table);
						Join newJoin = new Join();
						
						List<SelectItem<?>> selectItems = new ArrayList<>();
						for(SelectItem<?> c : columnsInvolved.getCorrelatedColums())
							selectItems.add(c);
	
						for(Column c : _columns)
							selectItems.add(new SelectItem<>(c));
	
						_outerJoin.setSelectItems(selectItems);
	
						// Create a new SubSelect
						ParenthesedSelect subSelect = new ParenthesedSelect();
						
						selectItems = plainSelect.getSelectItems();
						for(SelectItem<?> item : selectItems){
							if(item.getExpression() instanceof Column){
								Column _c = (Column) item.getExpression();
								for(SelectItem<?> c : columnsInvolved.getColumns())
									if(!ph.areColumnsEquals(c, new SelectItem<>(_c)))
										selectItems.add(c);
							}
						}
						
						plainSelect.setSelectItems(selectItems);
						
						// Set the PlainSelect as the subquery
						subSelect.setSelect(plainSelect);
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
						for(SelectItem<?> si : plainSelect.getSelectItems()){
							newGroupByElement.addGroupByExpressions(si.getExpression());
						}
						_outerJoin.setGroupByElement(newGroupByElement);
	
						ParenthesedSelect _subSelect = new ParenthesedSelect();
						_subSelect.setSelect(_outerJoin);
						_subSelect.setAlias(new Alias("nestedT" + count));
						
						pe.setSelect(_subSelect);
						pe.setJoinTable(_table);
	
						pe.setJoinExpressions(ph.equiJoins(_joinExp, _table,"nestedT" + count));
					}
				}
			}
			parserExpressions.add(pe);
			count++;
			return null;
		}

		@Override
		public <S> Void visit(Select select, S context) {
			// Your code here
			
			//super.visit(select, context);
			if(select instanceof ParenthesedSelect){
				ParenthesedSelect parenthesedSelect = (ParenthesedSelect) select;
				if(parenthesedSelect.getSelect() instanceof PlainSelect){
					List<Table> tables = new ArrayList<>();
					ParserHelper ph = new ParserHelper();
					PlainSelect plainSelect = (PlainSelect) parenthesedSelect.getSelect();
	
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
	
					for(SelectItem<?> c : columnsInvolved.getColumns())
						plainSelect.addSelectItems(c);
	
					// Create a new SubSelect
					ParenthesedSelect newSubSelect = new ParenthesedSelect();
	
					// Set the PlainSelect as the subquery
					newSubSelect.setSelect(plainSelect);
	
					newSubSelect.setAlias(new Alias("C" + count));
					
					boolean hasGroupByEle = false;


					ExpressionList<Column> groupByList = new ExpressionList<>();
					GroupByElement newGroupByElement = new GroupByElement();
					for(SelectItem<?> si : plainSelect.getSelectItems()){
						
						if(si.getExpression() instanceof Column){
							hasGroupByEle = true;
							groupByList.add((Column) si.getExpression());
						}
					}
					
					if(hasGroupByEle){
						newGroupByElement.setGroupByExpressions(groupByList);
						plainSelect.setGroupByElement(newGroupByElement);
					}
	
					List<Expression> joinExp = new ArrayList<>();
					
					if(columnsInvolved.getExpressions() != null) 
						joinExp.addAll(columnsInvolved.getExpressions());
					joinExp.add(_newJoin);
	
					pe.setSelect(newSubSelect);
					//pe.setJoinTable(tables.getFirst());
					pe.setJoinTable(null);
					pe.setWhereExpressions(joinExp);
					//pe.setJoinExpressions(joinExp);
					parserExpressions.add(pe);
	
					count++;
				}
			}

			return null;
		}
		
		@Override
		public <S> Void visit(GreaterThan greaterThan, S context) {
			Expression joinCondition = null;

			if(greaterThan.getRightExpression() instanceof AnyComparisonExpression){

				ParenthesedSelect subSelect = (ParenthesedSelect) ((AnyComparisonExpression) greaterThan.getRightExpression()).getSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelect();

				int i = 0;

				

				if(((AnyComparisonExpression) greaterThan.getRightExpression()).getAnyType() == AnyType.ALL){
					List<SelectItem<?>> columnList = new ArrayList<>();
					for(SelectItem<?> si : plainSelect.getSelectItems()){
						Function maxF = new Function();
						maxF.setName("MAX");
						maxF.setParameters(si.getExpression());

						SelectItem<?> newItem = new SelectItem<>(maxF);
						newItem.setAlias(new Alias("MAX"+i));
						columnList.add(newItem);

						i++;
					 }

					if(greaterThan.getLeftExpression() instanceof Column){
						_newJoin = new GreaterThan();
						((BinaryExpression) _newJoin).setLeftExpression(greaterThan.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("AllT"+count), columnList.get(0).getAlias().getName()));
					}else if(greaterThan.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) greaterThan.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							BinaryExpression gth = new GreaterThan();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("AllT"+count),columnList.get(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("AllT"+count),columnList.get(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}

					plainSelect.setSelectItems(columnList);
				}else{
					if(greaterThan.getLeftExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new GreaterThan();
						((BinaryExpression) _newJoin).setLeftExpression(greaterThan.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(greaterThan.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) greaterThan.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new GreaterThan();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
			 }else if(greaterThan.getRightExpression() instanceof ParenthesedSelect || greaterThan.getLeftExpression() instanceof ParenthesedSelect){
				PlainSelect plainSelect = null;
				if(greaterThan.getRightExpression() instanceof ParenthesedSelect){
					plainSelect = (PlainSelect) ((ParenthesedSelect) greaterThan.getRightExpression()).getSelect();
					
					if(greaterThan.getLeftExpression() instanceof Column){
						Column column = null;
						
						if(plainSelect.getSelectItems().get(0).getExpression() instanceof Column)
							column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						else{
							column = null;
							if(plainSelect.getSelectItems().get(0).getAlias() == null)
							plainSelect.getSelectItems().get(0).setAlias(new Alias("col0"));
						}
						// É UMA CONTA POR ISSO DÁ ERRO É NECESSÁRIO DAR NOME A COLUNA
						_newJoin = new GreaterThan();
						((BinaryExpression) _newJoin).setLeftExpression(greaterThan.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(greaterThan.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) greaterThan.getLeftExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new GreaterThan();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
				else{
					plainSelect = (PlainSelect) ((ParenthesedSelect) greaterThan.getLeftExpression()).getSelect();
					if(greaterThan.getRightExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new GreaterThan();
						((BinaryExpression) _newJoin).setLeftExpression(greaterThan.getRightExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(greaterThan.getRightExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) greaterThan.getRightExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new GreaterThan();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
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
			super.visit(greaterThan, context);	
			return null;
		}

		@Override
		public <S> Void visit(GreaterThanEquals greaterThanEquals, S context) {
			Expression joinCondition = null;

			if(greaterThanEquals.getRightExpression() instanceof AnyComparisonExpression){

				ParenthesedSelect subSelect = (ParenthesedSelect) ((AnyComparisonExpression) greaterThanEquals.getRightExpression()).getSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelect();

				int i = 0;

				

				if(((AnyComparisonExpression) greaterThanEquals.getRightExpression()).getAnyType() == AnyType.ALL){
					List<SelectItem<?>> columnList = new ArrayList<>();
					for(SelectItem<?> si : plainSelect.getSelectItems()){
						Function maxF = new Function();
						maxF.setName("MAX");
						maxF.setParameters(si.getExpression());

						SelectItem<?> newItem = new SelectItem<>(maxF);
						newItem.setAlias(new Alias("MAX"+i));
						columnList.add(newItem);

						i++;
					 }

					if(greaterThanEquals.getLeftExpression() instanceof Column){
						_newJoin = new GreaterThanEquals();
						((BinaryExpression) _newJoin).setLeftExpression(greaterThanEquals.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("AllT"+count), columnList.get(0).getAlias().getName()));
					}else if(greaterThanEquals.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) greaterThanEquals.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							BinaryExpression gth = new GreaterThanEquals();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("AllT"+count),columnList.get(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("AllT"+count),columnList.get(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}

					plainSelect.setSelectItems(columnList);
				}else{
					if(greaterThanEquals.getLeftExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new GreaterThanEquals();
						((BinaryExpression) _newJoin).setLeftExpression(greaterThanEquals.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(greaterThanEquals.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) greaterThanEquals.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new GreaterThanEquals();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
			 }else if(greaterThanEquals.getRightExpression() instanceof ParenthesedSelect || greaterThanEquals.getLeftExpression() instanceof ParenthesedSelect){
				PlainSelect plainSelect = null;
				if(greaterThanEquals.getRightExpression() instanceof ParenthesedSelect){
					plainSelect = (PlainSelect) ((ParenthesedSelect) greaterThanEquals.getRightExpression()).getSelect();
					
					if(greaterThanEquals.getLeftExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new GreaterThanEquals();
						((BinaryExpression) _newJoin).setLeftExpression(greaterThanEquals.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(greaterThanEquals.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) greaterThanEquals.getLeftExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new GreaterThanEquals();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
				else{
					plainSelect = (PlainSelect) ((ParenthesedSelect) greaterThanEquals.getLeftExpression()).getSelect();
					if(greaterThanEquals.getRightExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new GreaterThanEquals();
						((BinaryExpression) _newJoin).setLeftExpression(greaterThanEquals.getRightExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(greaterThanEquals.getRightExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) greaterThanEquals.getRightExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new GreaterThanEquals();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
			}else{
				if(newWhere == null)
					newWhere = greaterThanEquals;
				else 
					if (isInAndExpression)
						newWhere = new AndExpression(newWhere, greaterThanEquals);
					else if (isInOrExpression)
						newWhere = new OrExpression(newWhere, greaterThanEquals);
			}
			super.visit(greaterThanEquals, context);	
			return null;
		}

		@Override
		public <S> Void visit(MinorThan minorThan, S context) {
			Expression joinCondition = null;

			if(minorThan.getRightExpression() instanceof AnyComparisonExpression){

				ParenthesedSelect subSelect = (ParenthesedSelect) ((AnyComparisonExpression) minorThan.getRightExpression()).getSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelect();

				int i = 0;

				

				if(((AnyComparisonExpression) minorThan.getRightExpression()).getAnyType() == AnyType.ALL){
					List<SelectItem<?>> columnList = new ArrayList<>();
					for(SelectItem<?> si : plainSelect.getSelectItems()){
						Function maxF = new Function();
						maxF.setName("MIN");
						maxF.setParameters(si.getExpression());

						SelectItem<?> newItem = new SelectItem<>(maxF);
						newItem.setAlias(new Alias("MIN"+i));
						columnList.add(newItem);

						i++;
					 }

					if(minorThan.getLeftExpression() instanceof Column){
						_newJoin = new MinorThan();
						((BinaryExpression) _newJoin).setLeftExpression(minorThan.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("AllT"+count), columnList.get(0).getAlias().getName()));
					}else if(minorThan.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) minorThan.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							BinaryExpression gth = new MinorThan();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("AllT"+count),columnList.get(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("AllT"+count),columnList.get(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}

					plainSelect.setSelectItems(columnList);
				}else{
					if(minorThan.getLeftExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new MinorThan();
						((BinaryExpression) _newJoin).setLeftExpression(minorThan.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(minorThan.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) minorThan.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new MinorThan();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
			 }else if(minorThan.getRightExpression() instanceof ParenthesedSelect || minorThan.getLeftExpression() instanceof ParenthesedSelect){
				PlainSelect plainSelect = null;
				if(minorThan.getRightExpression() instanceof ParenthesedSelect){
					plainSelect = (PlainSelect) ((ParenthesedSelect) minorThan.getRightExpression()).getSelect();
					
					if(minorThan.getLeftExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new MinorThan();
						((BinaryExpression) _newJoin).setLeftExpression(minorThan.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(minorThan.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) minorThan.getLeftExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new MinorThan();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
				else{
					plainSelect = (PlainSelect) ((ParenthesedSelect) minorThan.getLeftExpression()).getSelect();
					if(minorThan.getRightExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new MinorThan();
						((BinaryExpression) _newJoin).setLeftExpression(minorThan.getRightExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(minorThan.getRightExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) minorThan.getRightExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new MinorThan();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
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
			super.visit(minorThan, context);	
			return null;
		}

		@Override
		public <S> Void visit(MinorThanEquals minorThanEquals, S context) {
			Expression joinCondition = null;

			if(minorThanEquals.getRightExpression() instanceof AnyComparisonExpression){

				ParenthesedSelect subSelect = (ParenthesedSelect) ((AnyComparisonExpression) minorThanEquals.getRightExpression()).getSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelect();

				int i = 0;

				

				if(((AnyComparisonExpression) minorThanEquals.getRightExpression()).getAnyType() == AnyType.ALL){
					List<SelectItem<?>> columnList = new ArrayList<>();
					for(SelectItem<?> si : plainSelect.getSelectItems()){
						Function maxF = new Function();
						maxF.setName("MIN");
						maxF.setParameters(si.getExpression());

						SelectItem<?> newItem = new SelectItem<>(maxF);
						newItem.setAlias(new Alias("MIN"+i));
						columnList.add(newItem);

						i++;
					 }

					if(minorThanEquals.getLeftExpression() instanceof Column){
						_newJoin = new MinorThanEquals();
						((BinaryExpression) _newJoin).setLeftExpression(minorThanEquals.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("AllT"+count), columnList.get(0).getAlias().getName()));
					}else if(minorThanEquals.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) minorThanEquals.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							BinaryExpression gth = new MinorThanEquals();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("AllT"+count),columnList.get(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("AllT"+count),columnList.get(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}

					plainSelect.setSelectItems(columnList);
				}else{
					if(minorThanEquals.getLeftExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new MinorThanEquals();
						((BinaryExpression) _newJoin).setLeftExpression(minorThanEquals.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(minorThanEquals.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) minorThanEquals.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new MinorThanEquals();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
			 }else if(minorThanEquals.getRightExpression() instanceof ParenthesedSelect || minorThanEquals.getLeftExpression() instanceof ParenthesedSelect){
				PlainSelect plainSelect = null;
				if(minorThanEquals.getRightExpression() instanceof ParenthesedSelect){
					plainSelect = (PlainSelect) ((ParenthesedSelect) minorThanEquals.getRightExpression()).getSelect();
					
					if(minorThanEquals.getLeftExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new MinorThanEquals();
						((BinaryExpression) _newJoin).setLeftExpression(minorThanEquals.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(minorThanEquals.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) minorThanEquals.getLeftExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new MinorThanEquals();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
				else{
					plainSelect = (PlainSelect) ((ParenthesedSelect) minorThanEquals.getLeftExpression()).getSelect();
					if(minorThanEquals.getRightExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new GreaterThan();
						((BinaryExpression) _newJoin).setLeftExpression(minorThanEquals.getRightExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(minorThanEquals.getRightExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) minorThanEquals.getRightExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new GreaterThan();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
			}else{
				if(newWhere == null)
					newWhere = minorThanEquals;
				else 
					if (isInAndExpression)
						newWhere = new AndExpression(newWhere, minorThanEquals);
					else if (isInOrExpression)
						newWhere = new OrExpression(newWhere, minorThanEquals);
			}
			super.visit(minorThanEquals, context);	
			return null;
		}

		@Override
		public <S> Void visit(EqualsTo equalsTo, S context) {
			Expression joinCondition = null;

			if(equalsTo.getRightExpression() instanceof AnyComparisonExpression){

				ParenthesedSelect subSelect = (ParenthesedSelect) ((AnyComparisonExpression) equalsTo.getRightExpression()).getSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelect();

				int i = 0;

				

				if(((AnyComparisonExpression) equalsTo.getRightExpression()).getAnyType() == AnyType.ALL){
					List<SelectItem<?>> columnList = new ArrayList<>();
					for(SelectItem<?> si : plainSelect.getSelectItems()){
						Function maxF = new Function();
						maxF.setName("MAX");
						maxF.setParameters(si.getExpression());

						SelectItem<?> newItem = new SelectItem<>(maxF);
						newItem.setAlias(new Alias("MAX"+i));
						columnList.add(newItem);

						i++;
					 }

					if(equalsTo.getLeftExpression() instanceof Column){
						_newJoin = new EqualsTo();
						((BinaryExpression) _newJoin).setLeftExpression(equalsTo.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("AllT"+count), columnList.get(0).getAlias().getName()));
					}else if(equalsTo.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) equalsTo.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							BinaryExpression gth = new EqualsTo();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("AllT"+count),columnList.get(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("AllT"+count),columnList.get(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}

					plainSelect.setSelectItems(columnList);
				}else{
					if(equalsTo.getLeftExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new EqualsTo();
						((BinaryExpression) _newJoin).setLeftExpression(equalsTo.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(equalsTo.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) equalsTo.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new EqualsTo();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
			 }else if(equalsTo.getRightExpression() instanceof ParenthesedSelect || equalsTo.getLeftExpression() instanceof ParenthesedSelect){
				PlainSelect plainSelect = null;
				if(equalsTo.getRightExpression() instanceof ParenthesedSelect){
					plainSelect = (PlainSelect) ((ParenthesedSelect) equalsTo.getRightExpression()).getSelect();
					
					if(equalsTo.getLeftExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new EqualsTo();
						((BinaryExpression) _newJoin).setLeftExpression(equalsTo.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(equalsTo.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) equalsTo.getLeftExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new EqualsTo();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
				else{
					plainSelect = (PlainSelect) ((ParenthesedSelect) equalsTo.getLeftExpression()).getSelect();
					if(equalsTo.getRightExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new GreaterThan();
						((BinaryExpression) _newJoin).setLeftExpression(equalsTo.getRightExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(equalsTo.getRightExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) equalsTo.getRightExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new GreaterThan();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
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
			super.visit(equalsTo, context);	
			return null;
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
		public <S> Void visit(NotEqualsTo notEqualsTo, S context) {
			Expression joinCondition = null;

			if(notEqualsTo.getRightExpression() instanceof AnyComparisonExpression){

				ParenthesedSelect subSelect = (ParenthesedSelect) ((AnyComparisonExpression) notEqualsTo.getRightExpression()).getSelect();
				PlainSelect plainSelect = (PlainSelect) subSelect.getSelect();

				int i = 0;

				

				if(((AnyComparisonExpression) notEqualsTo.getRightExpression()).getAnyType() == AnyType.ALL){
					List<SelectItem<?>> columnList = new ArrayList<>();
					for(SelectItem<?> si : plainSelect.getSelectItems()){
						Function maxF = new Function();
						maxF.setName("MAX");
						maxF.setParameters(si.getExpression());

						SelectItem<?> newItem = new SelectItem<>(maxF);
						newItem.setAlias(new Alias("MAX"+i));
						columnList.add(newItem);

						i++;
					 }

					if(notEqualsTo.getLeftExpression() instanceof Column){
						_newJoin = new NotEqualsTo();
						((BinaryExpression) _newJoin).setLeftExpression(notEqualsTo.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("AllT"+count), columnList.get(0).getAlias().getName()));
					}else if(notEqualsTo.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) notEqualsTo.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							BinaryExpression gth = new NotEqualsTo();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("AllT"+count),columnList.get(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("AllT"+count),columnList.get(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}

					plainSelect.setSelectItems(columnList);
				}else{
					if(notEqualsTo.getLeftExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new NotEqualsTo();
						((BinaryExpression) _newJoin).setLeftExpression(notEqualsTo.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(notEqualsTo.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) notEqualsTo.getLeftExpression();

						for (i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new NotEqualsTo();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
			 }else if(notEqualsTo.getRightExpression() instanceof ParenthesedSelect || notEqualsTo.getLeftExpression() instanceof ParenthesedSelect){
				PlainSelect plainSelect = null;
				if(notEqualsTo.getRightExpression() instanceof ParenthesedSelect){
					plainSelect = (PlainSelect) ((ParenthesedSelect) notEqualsTo.getRightExpression()).getSelect();
					
					if(notEqualsTo.getLeftExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new NotEqualsTo();
						((BinaryExpression) _newJoin).setLeftExpression(notEqualsTo.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(notEqualsTo.getLeftExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) notEqualsTo.getLeftExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new NotEqualsTo();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
				else{
					plainSelect = (PlainSelect) ((ParenthesedSelect) notEqualsTo.getLeftExpression()).getSelect();
					if(notEqualsTo.getRightExpression() instanceof Column){
						Column column = (Column) plainSelect.getSelectItems().get(0).getExpression();
						_newJoin = new GreaterThan();
						((BinaryExpression) _newJoin).setLeftExpression(notEqualsTo.getRightExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(0).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(0).getAlias().getName()));
					}else if(notEqualsTo.getRightExpression() instanceof ParenthesedExpressionList){
						ParenthesedExpressionList<?> pel = (ParenthesedExpressionList<?>) notEqualsTo.getRightExpression();

						for (int i = 0; i < pel.size(); i++) {
							Column column = (Column) plainSelect.getSelectItems().get(i).getExpression();
							BinaryExpression gth = new GreaterThan();
							gth.setLeftExpression(pel.get(i));
							gth.setRightExpression(new Column(new Table("C"+count), plainSelect.getSelectItem(i).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(i).getAlias().getName()));
			
							// Create nested equals conditions for previous columns
							Expression exp = gth;
							for (int j = i - 1; j >= 0; j--) {
								exp = new AndExpression(
									new EqualsTo(pel.get(j), new Column(new Table("C"+count),plainSelect.getSelectItem(j).getAlias() == null ? column.getColumnName() : plainSelect.getSelectItem(j).getAlias().getName())), exp
								);
							}
			
							if (joinCondition == null) {
								joinCondition = exp;
							} else {
								joinCondition = new OrExpression(joinCondition, new ParenthesedExpressionList<>(exp));
							}
						}
						_newJoin = joinCondition;						
					}
				}
			}else{
				if(newWhere == null)
					newWhere = notEqualsTo;
				else 
					if (isInAndExpression)
						newWhere = new AndExpression(newWhere, notEqualsTo);
					else if (isInOrExpression)
						newWhere = new OrExpression(newWhere, notEqualsTo);
			}
			super.visit(notEqualsTo, context);	
			return null;
		}

		@Override
		public <S> Void visit(AndExpression andExpression, S context) {
			isInAndExpression = true;
			super.visit(andExpression, context);
			return null;
		}
		
		@Override
		public <S> Void visit(OrExpression orExpression, S context) {
			isInOrExpression = true;
			super.visit(orExpression, context);
			return null;
		}

		public List<ParserExpression> getParserExpressions() {
			return parserExpressions.reversed();
		}

		public Expression getNewWhere() {
			return newWhere;
		}
	}

	private static class ColumnsInvolved extends ExpressionVisitorAdapter<Object> {
		private List<Table> tables;
		private List<SelectItem<?>> columns;
		private List<SelectItem<?>> correlatedColums;
		//private List<Expression> expressions;
		private ExpressionList<Expression> expressions;
		private Expression newWhere = null;
		private Table tableJoin;
		private boolean isInAndExpression;
		private boolean isInOrExpression;
		private boolean isAll = false;

		public ColumnsInvolved(List<Table> tables){
			this.tables = tables;
			columns = new ArrayList<>();
			correlatedColums = new ArrayList<>();
			//expressions = new ArrayList<>();
			expressions = new ExpressionList<>();

		}

		private boolean verifyOuterTable(Column column){
			ParserHelper ph = new ParserHelper();
			boolean isOuterTable = true;
			for(Table table : tables){		
				if(column.getTable() == null)
					isOuterTable = false;
				else if(ph.areTablesEqual(table, column.getTable()))
					isOuterTable = false;
			}
			return isOuterTable;
		}

		private void createExpression(Expression exp1, Expression exp2, Object comparison){
			Column col1 = (Column) exp1;
			Column col2 = (Column) exp2;

			columns.add(new SelectItem<>(new Column().withColumnName(col2.getColumnName()).withTable(col2.getTable() != null ? new Table(col2.getTable().getFullyQualifiedName()) : null)));
			correlatedColums.add(new SelectItem<>(new Column().withColumnName(col1.getColumnName()).withTable(col1.getTable())));
			col2.setTable(new Table(isAll ? "AllT"+count : "C"+count));

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
		public <S> Void visit(AndExpression andExpression, S context) {
			isInAndExpression = true;
			super.visit(andExpression, context);
			return null;
		}
		
		@Override
		public <S> Void visit(OrExpression orExpression, S context) {
			isInOrExpression = true;
			super.visit(orExpression, context);
			return null;
		}

		@Override
		public <S> Void visit(EqualsTo equalsTo, S context) {
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
			super.visit(equalsTo, context);
			return null;
		}

		@Override
		public <S> Void visit(NotEqualsTo NoEqualsTo, S context) {
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
			super.visit(NoEqualsTo, context);
			return null;
		}

		@Override
		public <S> Void visit(GreaterThan greaterThan, S context) {
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
			super.visit(greaterThan, context);
			return null;
		}

		@Override
		public <S> Void visit(GreaterThanEquals greaterThan, S context) {
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
			super.visit(greaterThan, context);
			return null;
		}

		@Override
		public <S> Void visit(MinorThan minorThan, S context) {
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
			super.visit(minorThan, context);
			return null;
		}

		@Override
		public <S> Void visit(MinorThanEquals minorThan, S context) {
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
			super.visit(minorThan, context);
			return null;
		}

		@Override
		public <S> Void visit(IsNullExpression isNullExpression, S context) {
			if(newWhere == null)
				newWhere = isNullExpression;
			else 
				if (isInAndExpression)
					newWhere = new AndExpression(newWhere, isNullExpression);
				else if (isInOrExpression)
					newWhere = new OrExpression(newWhere, isNullExpression);
			super.visit(isNullExpression);
			return null;
		}

		@Override
		public <S> Void visit(LikeExpression likeExpression, S context) {
			if(newWhere == null)
				newWhere = likeExpression;
			else 
				if (isInAndExpression)
					newWhere = new AndExpression(newWhere, likeExpression);
				else if (isInOrExpression)
					newWhere = new OrExpression(newWhere, likeExpression);

			super.visit(likeExpression, context);
			return null;
		}

		@Override
		public <S> Void visit(InExpression inExpression, S context) {
			if(newWhere == null)
				newWhere = inExpression;
			else 
				if (isInAndExpression)
					newWhere = new AndExpression(newWhere, inExpression);
				else if (isInOrExpression)
					newWhere = new OrExpression(newWhere, inExpression);

			super.visit(inExpression, context);
			return null;
		}

		/* Perceber em que query é necessário
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
		*/

		public Expression getNewWhere() {
			return newWhere;
		}

		public ExpressionList<Expression> getExpressions() {
			return expressions;
		}

		public List<SelectItem<?>> getCorrelatedColums() {
			return correlatedColums;
		}

		public List<SelectItem<?>> getColumns() {
			return columns;
		}

		public Table getJoinTable() {
			return tableJoin;
		}

		public void setAll(boolean isAll){
			this.isAll = isAll;
		}
	}
}