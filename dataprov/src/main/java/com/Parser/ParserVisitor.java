package com.Parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.AnalyticExpression;
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
import net.sf.jsqlparser.expression.WindowDefinition;
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
import net.sf.jsqlparser.util.TablesNamesFinder;

import com.Helper.ListAggColumns;
import com.Helper.ParserHelper;

public class ParserVisitor {
	private static int count = 0;

	public static class FunctionProjection implements SelectItemVisitor<Object> {
		private boolean hasFunction = false;
		private String aggExpression = "";
		private boolean minMax = false;
		private Column minMaxColumn = null;
		private String replaceStr = "";
		private boolean coalesce = false;
		private ListAggColumns aggColumns = new ListAggColumns();

		@Override
		public <S> Object visit(SelectItem<? extends Expression> selectItem, S context) {
			if(selectItem.getExpression() instanceof Function){
				Function function = (Function) selectItem.getExpression();

				if(function.getName().toLowerCase().equals("count")){
					hasFunction = true;
					if(coalesce && function.getParameters().toString().compareTo("*") != 0){
						aggExpression += "|| (case when "+function.getParameters().toString()+" is null then '' else (' .count ' || CAST(1 as varchar)) end)";
					}else
						aggExpression += "|| ' .count ' || CAST(1 as varchar)";
					
					//Code Yael
					if(selectItem.getAlias() == null)
						selectItem.setAlias(new Alias("count_"));
					aggColumns.addAggColumns(function.getParameters().toString(), "count", selectItem.getAlias().getName());
				}else if(function.getName().toLowerCase().equals("sum")){
					if(hasColumns(function.getParameters())){
						hasFunction = true;
						aggExpression += "|| ' .sum ' || CAST("+function.getParameters().toString()+" as varchar)";
					}
					//Code Yael
					if(selectItem.getAlias() == null)
						selectItem.setAlias(new Alias("sum_"));
					aggColumns.addAggColumns(function.getParameters().toString(), "sum", selectItem.getAlias().getName());
				}else if(function.getName().toLowerCase().equals("avg")){
					/*
					 * The need of '/avgt' and the replace is due to the monoids calculation being correct. With this change the result of AVG will be correct although it will be present as 'NUM/NUM'
					 */
					if(hasColumns(function.getParameters())){
						hasFunction = true;
						aggExpression += "|| ' .avg ' || CAST("+function.getParameters().toString()+" as varchar) || '/avgt'";
						if(replaceStr == "")
							replaceStr = "CAST(COUNT(*) as varchar)";
					}
					//Code Yael
					aggColumns.addAggColumns(function.getParameters().toString(), "avg", selectItem.getAlias().getName());
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
						aggExpression += "|| ' "+ (function.getName().toLowerCase().equals("min") ? ".min" : ".max") +" ' || CAST("+function.getParameters().toString()+" as varchar)";
					}

					aggColumns.addAggColumns(function.getParameters().toString(), (function.getName().toLowerCase().equals("min") ? "min" : "max"), selectItem.getAlias().getName());
				}

			}else if(selectItem.getExpression() instanceof Division){
				Division divison = (Division) selectItem.getExpression();
				if(divison.getLeftExpression() instanceof Function){
					Function function = (Function) divison.getLeftExpression();
					if(divison.getRightExpression() instanceof DoubleValue)
						aggColumns.addAggColumns(function.getParameters().toString()+'/'+divison.getRightExpression().toString(), function.getName().toLowerCase(), selectItem.getAlias().getName());
					else 
						aggColumns.addAggColumns(function.getParameters().toString(), function.getName().toLowerCase(), selectItem.getAlias().getName());
				}else if(divison.getLeftExpression() instanceof Multiplication){
					Multiplication multiplication = (Multiplication) divison.getLeftExpression();
					Object obj = multiplication.getLeftExpression();
					Object obj2 = multiplication.getRightExpression();

					if(obj instanceof Function && !(obj2 instanceof Function)){
						Function function = (Function) obj;
						aggColumns.addAggColumns(function.getParameters().toString() + '*' + obj2.toString(), function.getName().toLowerCase(), selectItem.getAlias().getName());
					}else if(obj2 instanceof Function && !(obj instanceof Function)){
						Function function = (Function) obj2;
						aggColumns.addAggColumns(function.getParameters().toString() + '*' + obj.toString(), function.getName().toLowerCase(), selectItem.getAlias().getName());
					}
				}

				if(divison.getRightExpression() instanceof Function){
					Function function = (Function) divison.getRightExpression();
					aggColumns.addAggColumns(function.getParameters().toString(), function.getName().toLowerCase(), selectItem.getAlias().getName());
				}		
			
			}else if(selectItem.getExpression() instanceof Multiplication){
				Multiplication multiplication = (Multiplication) selectItem.getExpression();
				Object obj = multiplication.getLeftExpression();
				Object obj2 = multiplication.getRightExpression();

				if(obj instanceof Function && !(obj2 instanceof Function)){
					Function function = (Function) obj;
					aggColumns.addAggColumns(function.getParameters().toString() + '*' + obj2.toString(), function.getName().toLowerCase(), selectItem.getAlias().getName());
				}else if(obj2 instanceof Function && !(obj instanceof Function)){
					Function function = (Function) obj2;
					aggColumns.addAggColumns(function.getParameters().toString() + '*' + obj.toString(), function.getName().toLowerCase(), selectItem.getAlias().getName());
				}
			}else if(selectItem.getExpression() instanceof Addition || selectItem.getExpression() instanceof Subtraction){
				String tempAggexp = arithmeticFuncs(selectItem.getExpression());
				if(tempAggexp != ""){
					hasFunction = true;
					aggExpression += "|| ' . ' || CAST("+tempAggexp+" as varchar)";
					if(replaceStr != "")
					aggExpression += "|| '/avgt'";
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
					if(function.getName().toLowerCase().equals("sum") || function.getName().toLowerCase().equals("count")){
						if(hasColumns(function.getParameters())){
							// + function.getParameters().toString();
							replaceStr = "CAST("+function.toString()+" as varchar)";
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
					if(function.getName().toLowerCase().equals("avg")){
							arithExp += " * "+function.getParameters().toString();
							if(replaceStr == "")
								replaceStr = "CAST(COUNT(*) as varchar)";
							_hasFunction = true;
					}else if(function.getName().toLowerCase().equals("sum")){
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

		public String getReplaceStr() {
			return replaceStr;
		}

		public void setCoalesce(boolean coalesce) {
			this.coalesce = coalesce;
		}

		public ListAggColumns getAggColumns() {
			return aggColumns;
		}
	}

	public static class JoinOnVisitor extends ExpressionVisitorAdapter<Object>{
		private String symbolicExpression = "";
		private boolean originalResult;
		private PlainSelect plainSelect;

		public JoinOnVisitor(boolean originalResult, PlainSelect plainSelect){
			this.originalResult = originalResult;
			this.plainSelect = plainSelect;
		}

		public <S> Void visit(EqualsTo equalsTo, S context) {
			String symbExpTemp = createSymExp(equalsTo.getLeftExpression(), equalsTo.getRightExpression(), "=");
			if(symbExpTemp != ""){
				symbolicExpression += symbExpTemp;
				//equalsTo = new EqualsTo(new LongValue(1), new LongValue(1));
				if(!originalResult){
					equalsTo.setLeftExpression(new LongValue(1));
					equalsTo.setRightExpression(new LongValue(1));
				}
			}

			return null;
		}

		public <S> Void visit(GreaterThan greaterThan, S context) {
			String symbExpTemp = createSymExp(greaterThan.getLeftExpression(), greaterThan.getRightExpression(), ">");
			if(symbExpTemp != ""){
				symbolicExpression += symbExpTemp;
				//equalsTo = new EqualsTo(new LongValue(1), new LongValue(1));
				if(!originalResult){
					greaterThan.setLeftExpression(new LongValue(1));
					greaterThan.setRightExpression(new LongValue(1));
				}
			}

			return null;
		}

		public <S> Void visit(MinorThan minorThan, S context) {
			String symbExpTemp = createSymExp(minorThan.getLeftExpression(), minorThan.getRightExpression(), "<");
			if(symbExpTemp != ""){
				symbolicExpression += symbExpTemp;
				//equalsTo = new EqualsTo(new LongValue(1), new LongValue(1));
				if(!originalResult){
					minorThan.setLeftExpression(new LongValue(1));
					minorThan.setRightExpression(new LongValue(1));
				}
			}

			return null;
		}

		private String createSymExp(Expression left, Expression right, String operator){
			if(left instanceof Column && right instanceof Column){
				Column columnL = (Column) left;
				Column columnR = (Column) right;

				if(isFunc(columnL) && isFunc(columnR)){
					if (originalResult) 
						return "'. [' || " + columnL.getFullyQualifiedName(false) + "_agg" + "|| '" + operator + "' || " + columnR.getFullyQualifiedName(false) + "_agg" + "|| ']'";
					else
						return "'. [' || " + columnL.getFullyQualifiedName(false) + "|| '" + operator + "' || " + columnR.getFullyQualifiedName(false) + "|| ']'";
				}else if(isFunc(columnL) && !isFunc(columnR)){
					if (originalResult) 
						return "'. [' || " + columnL.getFullyQualifiedName(false) + "_agg" + "|| '" + operator +" 1 " + (char) 0x2297 + "' || " + columnR.getFullyQualifiedName(false) + "|| ']'";
					else
						return "'. [' || " + columnL.getFullyQualifiedName(false) + "|| '" + operator +" 1 " + (char) 0x2297 + "' || " + columnR.getFullyQualifiedName(false) + "|| ']'";
				}else if(!isFunc(columnL) && isFunc(columnR)){
					if (originalResult) 
						return "'. [' || " + columnR.getFullyQualifiedName(false) + "_agg" + "|| '" + operator +" 1 " + (char) 0x2297 + "' || " + columnL.getFullyQualifiedName(false) + "|| ']'";
					else
						return "'. [' || " + columnR.getFullyQualifiedName(false) + "|| '" + operator +" 1 " + (char) 0x2297 + "' || " + columnL.getFullyQualifiedName(false) + "|| ']'";
				}
			}else if(left instanceof Column && !(right instanceof Column) && isFunc((Column) left)){
				if (originalResult) 
						return "'. [' || " + ((Column) left).getFullyQualifiedName(false) + "_agg" + "|| '" + operator +" 1 " + (char) 0x2297 +  "' || " + right.toString() + "|| ']'";
					else
						return "'. [' || " + ((Column) left).getFullyQualifiedName(false) + "|| '" + operator +" 1 " + (char) 0x2297 + "' || " + right.toString() + "|| ']'";
			}else if(!(left instanceof Column) && right instanceof Column && isFunc((Column) right)){
				if (originalResult) 
						return "'. [' || " + ((Column) right).getFullyQualifiedName(false) + "_agg" + "|| '" + operator +" 1 " + (char) 0x2297 + "' || " + right.toString() + "|| ']'";
					else
						return "'. [' || " + ((Column) right).getFullyQualifiedName(false) + "|| '" + operator +" 1 " + (char) 0x2297 + "' || " + right.toString() + "|| ']'";
			}
			return "";

		}

		private boolean isFunc(Column column){
			try {
				List<String> columns = new ArrayList<>();
				Set<String> tableNames = TablesNamesFinder.findTables(plainSelect.toString());
				if(column.getTable() != null && tableNames.contains(column.getTable().getName()) || column.getTable() == null || column.getColumnName().toLowerCase().equals("prov")){
					return false;
				}else{
					if(plainSelect.getFromItem() instanceof ParenthesedSelect && plainSelect.getFromItem().getAlias().getName().equals(column.getTable().getName())){
						List<SelectItem<?>> selectItems = ((PlainSelect) ((ParenthesedSelect) plainSelect.getFromItem()).getSelect()).getSelectItems();
						
						for(SelectItem<?> item :selectItems)
						{
							if(item.getAlias() != null && item.getAlias().getName().equals(column.getColumnName())){
								if(item.getExpression() instanceof Function && (((Function) item.getExpression()).getName().toLowerCase().equals("count") || ((Function) item.getExpression()).getName().toLowerCase().equals("sum") || ((Function) item.getExpression()).getName().toLowerCase().equals("avg") || ((Function) item.getExpression()).getName().toLowerCase().equals("min") || ((Function) item.getExpression()).getName().toLowerCase().equals("max"))){
									return true;
								}else if(item.getExpression().toString().toLowerCase().contains("sum") || item.getExpression().toString().toLowerCase().contains("count") || item.getExpression().toString().toLowerCase().contains("avg") || item.getExpression().toString().toLowerCase().contains("min") || item.getExpression().toString().toLowerCase().contains("max")){
										return true;
								}else if(item.toString().contains(".prov")){
									return true;
								}else{
									return false;
								}
							}else if(item.getExpression() instanceof Column){
								columns.add(((Column) item.getExpression()).getColumnName());
							}
						}

						if(columns.contains(column.getColumnName()+"_agg")){
							return true;
						}
					}else{
						for(Join join : plainSelect.getJoins()){
							if(join.getRightItem() instanceof ParenthesedSelect && join.getRightItem().getAlias().getName().equals(column.getTable().getName())){
								List<SelectItem<?>> selectItems = ((PlainSelect) ((ParenthesedSelect) join.getRightItem()).getSelect()).getSelectItems();

								for(SelectItem<?> item :selectItems)
								{
									if(item.getAlias() != null && item.getAlias().getName().equals(column.getColumnName())){
										if(item.getExpression() instanceof Function && (((Function) item.getExpression()).getName().toLowerCase().equals("count") || ((Function) item.getExpression()).getName().toLowerCase().equals("sum") || ((Function) item.getExpression()).getName().toLowerCase().equals("avg") || ((Function) item.getExpression()).getName().toLowerCase().equals("min") || ((Function) item.getExpression()).getName().toLowerCase().equals("max"))){
											return true;
										}if(item.getExpression().toString().toLowerCase().contains("sum") || item.getExpression().toString().toLowerCase().contains("count") || item.getExpression().toString().toLowerCase().contains("avg") || item.getExpression().toString().toLowerCase().contains("min") || item.getExpression().toString().toLowerCase().contains("max")){
											return true;
										}else if(item.toString().contains(".prov")){
											return true;
										}else{
											return false;
										}
									}else if(item.getExpression() instanceof Column){
										columns.add(((Column) item.getExpression()).getColumnName());
									}
								}
		
								if(columns.contains(column.getColumnName()+"_agg")){
									return true;
								}
							}
						}
					}
				}
			} catch (JSQLParserException e) {
				return false;
			}
			return false;
		}

		public Expression removeOneOne(Expression expression) {
			if (expression == null) return null;
	
			if (isOneOne(expression)) {
				return null;
			}
	
			// Handle AND expressions
			if (expression instanceof AndExpression) {
				AndExpression andExpr = (AndExpression) expression;
				Expression left = removeOneOne(andExpr.getLeftExpression());
				Expression right = removeOneOne(andExpr.getRightExpression());
	
				if (left == null) return right; 
				if (right == null) return left; 
				return new AndExpression(left, right);
			}
	
			// Handle OR expressions
			if (expression instanceof OrExpression) {
				OrExpression orExpr = (OrExpression) expression;
				Expression left = removeOneOne(orExpr.getLeftExpression());
				Expression right = removeOneOne(orExpr.getRightExpression());
	
				if (left == null) return right;
				if (right == null) return left; 
				return new OrExpression(left, right);
			}
	
			return expression;
		}
	

		private static boolean isOneOne(Expression expression) {
			if (expression instanceof BinaryExpression) {
				BinaryExpression binExp = (BinaryExpression) expression;
				return isNumericLiteral(binExp.getLeftExpression(), 1) &&
					   isNumericLiteral(binExp.getRightExpression(), 1);
			}
			return false;
		}
	
		// Check if an expression is a numeric literal with a specific value
		private static boolean isNumericLiteral(Expression expr, int value) {
			if (expr instanceof LongValue) {
				return ((LongValue) expr).getValue() == value;
			}
			return false;
		}

		public String getSymbolicExpression() {
			return symbolicExpression;
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
		private boolean originalResult = false;
		private boolean havingExists = false;
		private Map<Expression, String> whereExpressions;
		private String currentOperator = "";
		
		public WhereVisitor(){
			parserExpressions = new ArrayList<>();
			_columns = new ArrayList<>();
			whereExpressions = new HashMap<>();
		}

		public boolean isHavingExists() {
			return havingExists;
		}

		public void setOriginalResult(boolean originalResult){
			this.originalResult = originalResult;
		}

		public WhereVisitor(List<Table> mainQueryTables){
			parserExpressions = new ArrayList<>();
			_columns = new ArrayList<>();
			this.mainQueryTables = mainQueryTables;
			whereExpressions = new HashMap<>();
		}

		public PlainSelect identifyOperators(PlainSelect plainSelect){
			ParserHelper ph = new ParserHelper();
			mainQueryTables = ph.extractJoinTables(plainSelect);
			WhereVisitor whereVisitor = new WhereVisitor(mainQueryTables);
			whereVisitor.setOriginalResult(originalResult);
			whereVisitor.splitConditionsWithOperators(plainSelect.getWhere(), "");
			plainSelect.getWhere().accept(whereVisitor);
			havingExists = whereVisitor.isHavingExists();
			if(whereVisitor.getParserExpressions().size() > 0)
				processingOperators(plainSelect, whereVisitor);

			return plainSelect;
		} 

		private PlainSelect processingOperators(PlainSelect plainSelect, WhereVisitor whereVisitor){
			ParserHelper ph = new ParserHelper();
			Expression newWhere = plainSelect.getWhere().accept(new WhereVisitorJoins(true), null);
			plainSelect = ph.transformJoins(plainSelect);
			plainSelect.setWhere(newWhere);
			for(ParserExpression pe : whereVisitor.getParserExpressions()){
				Join newJoin = new Join();
				newJoin.setRightItem(pe.getSelect());
				if(pe.getWhereExpressions() != null){
					newJoin.setLeft(true);
					if(plainSelect.getWhere() != null)
						plainSelect.setWhere(new AndExpression(plainSelect.getWhere(), ph.buildAndExpression(pe.getWhereExpressions())));
					else 
						plainSelect.setWhere(ph.buildAndExpression(pe.getWhereExpressions()));
				}else if(pe.getJoinExpression().size() == 0)
					newJoin.setSimple(true);
				else
					newJoin.setInner(true);
				newJoin.setOnExpressions(pe.getJoinExpression());
				if(plainSelect.getJoins() != null)
					plainSelect.getJoins().add(newJoin);
				else{
					List<Join> joins = new ArrayList<>();
					joins.add(newJoin);
					plainSelect.setJoins(joins);
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
				
				JoinConditionsNested joinConditionsNested = new JoinConditionsNested(tables, "C" + count, true);
				joinConditionsNested.splitConditionsWithOperators(plainSelect.getWhere(), "");
				
				plainSelect.setWhere(joinConditionsNested.getWhereExpression());

				plainSelect = ph.transformJoins(plainSelect);
				List<SelectItem<?>> selectItems = new ArrayList<>();
				for(Column c : joinConditionsNested.getColumns()){
					if(!c.getColumnName().toLowerCase().equals("prov")){
						selectItems.add(new SelectItem<>(c));
					}
				}

				plainSelect.setSelectItems(selectItems);

				List<Join> joins = new ArrayList<>();
				for(NestedTables nt : joinConditionsNested.getJoins()){
					Join newJoin = new Join();
					Table _table = null;
					for(Table t : mainQueryTables){
						if(t.getAlias() != null && t.getAlias().getName().equals(nt.getJoinTable().getName()))
						{
							String tableName = t.getName();
							_table = new Table();
							_table.setName(tableName);
						}
					}
					if(_table == null)
						_table = nt.getJoinTable();
					else
						_table.setAlias(nt.getJoinTable().getAlias());
					newJoin.setRightItem(_table);
					newJoin.setInner(true);
					List<Expression> joinExp = new ArrayList<>();
					joinExp.add(nt.getJoinExp());
					newJoin.setOnExpressions(joinExp);
					joins.add(newJoin);
				}

				plainSelect.setJoins(joins);

				ExpressionList<Column> groupByList = new ExpressionList<>();
				GroupByElement newGroupByElement = new GroupByElement();
				for(SelectItem<?> si : plainSelect.getSelectItems()){
					if(si.getExpression() instanceof Column)
						groupByList.add((Column) si.getExpression());
				}				
				newGroupByElement.setGroupByExpressions(groupByList);
				plainSelect.setGroupByElement(newGroupByElement);

				ParenthesedSelect subSelect = new ParenthesedSelect();
				subSelect.setSelect(plainSelect);
				subSelect.setAlias(new Alias("C" + count));

				ParserExpression pe = new ParserExpression();
				pe.setSelect(subSelect);
				ExpressionList<Expression> joinExpressions = new ExpressionList<Expression>();
				joinExpressions.add(joinConditionsNested.getExpJoins());
				pe.setJoinExpressions(joinExpressions);

				parserExpressions.add(pe);
				count++;
			}					
			return null;
		}

		/*@Override
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
		}*/

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
				
					JoinConditionsNested joinConditionsNested = new JoinConditionsNested(tables, "C" + count, false);
					joinConditionsNested.splitConditionsWithOperators(plainSelect.getWhere(), "");
				
					plainSelect.setWhere(joinConditionsNested.getWhereExpression());

					plainSelect = ph.transformJoins(plainSelect);

					SelectItem<?> firstSelectItem = plainSelect.getSelectItems().get(0);
					if (firstSelectItem.getExpression() instanceof AllColumns || firstSelectItem.getExpression() instanceof LongValue) {
						List<SelectItem<?>> selectItems = new ArrayList<>();
						for(Column c : joinConditionsNested.getColumns())
							selectItems.add(new SelectItem<>(c));
						plainSelect.setSelectItems(selectItems);
					}else{
						if(joinConditionsNested.getColumns().size() != plainSelect.getSelectItems().size()){
							List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
							for(Column c : joinConditionsNested.getColumns())
								selectItems.add(new SelectItem<>(c));
							plainSelect.setSelectItems(selectItems);
						}
					}
					ParserExpression pe = new ParserExpression();
					for(Column c : joinConditionsNested.getColumns()){
						IsNullExpression notNullExp = new IsNullExpression();
						Column colWhere = new Column();
						String columnName = c.getColumnName();
						colWhere.setColumnName(columnName);
						colWhere.setTable(new Table("C" + count));
						
						notNullExp.setLeftExpression(colWhere);
						notNullExp.setNot(false);
						pe.addWhereExpression(notNullExp);
					}

					existExp.setAlias(new Alias("C" + count));
					
					
					pe.setSelect(existExp);
					pe.setJoinExpressions(joinConditionsNested.getExpJoins());
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
				PlainSelect plainSelect = (PlainSelect) inExp.getSelect();
				Expression newExp = null;
				for(int i = 0; i < inColumns.size(); i++){
					EqualsTo eq = new EqualsTo();
					eq.setLeftExpression(inColumns.get(i));
					eq.setRightExpression(plainSelect.getSelectItems().get(i).getExpression());		
					if(newExp == null)
						newExp = eq;
					else
						newExp = new AndExpression(newExp, eq);
				}
				
				if(inExpression.isNot()){
					NotExpression notExistsExpression = new NotExpression();
            		ExistsExpression existsExpression = new ExistsExpression();
					
					plainSelect.setWhere(new AndExpression(plainSelect.getWhere(), newExp));
					existsExpression.setRightExpression(inExp);
					notExistsExpression.setExpression(existsExpression);
					notExistsExpression.accept(this, null);
					return null;
				}


				List<Table> tables = new ArrayList<>();
				ParserHelper ph = new ParserHelper();
			

				if(plainSelect.getWhere() != null){
					WhereVisitor whereVisitor = new WhereVisitor();
					plainSelect.getWhere().accept(whereVisitor);
					if(whereVisitor.getParserExpressions().size() > 0)
						plainSelect = whereVisitor.processingOperators(plainSelect, whereVisitor);
				}

				tables = ph.extractJoinTables(plainSelect);
				
				JoinConditionsNested joinConditionsNested = new JoinConditionsNested(tables, "C" + count, true);
				joinConditionsNested.splitConditionsWithOperators(plainSelect.getWhere(), "");
				
				if(joinConditionsNested.getColumns().size() > 0){
					ExistsExpression existsExpression = new ExistsExpression();
					plainSelect.setWhere(new AndExpression(plainSelect.getWhere(), newExp));
					existsExpression.setRightExpression(inExp);					
					existsExpression.accept(this, null);
					return null;
				}else{
					plainSelect.setWhere(joinConditionsNested.getWhereExpression());

					plainSelect = ph.transformJoins(plainSelect);

					ParserExpression pe = new ParserExpression();
					ParenthesedSelect subSelect = new ParenthesedSelect();

					// Set the PlainSelect as the subquery
					subSelect.setSelect(plainSelect);
					subSelect.setAlias(new Alias("C" + count));
					pe.setSelect(subSelect);

					newExp = null;
					for(int i = 0; i < inColumns.size(); i++){
						EqualsTo eq = new EqualsTo();
						eq.setLeftExpression(inColumns.get(i));
						String columnName = "";
						if(plainSelect.getSelectItems().get(i).getExpression() instanceof Column)
							columnName = ((Column) plainSelect.getSelectItems().get(i).getExpression()).getColumnName();
						else
							columnName = plainSelect.getSelectItems().get(i).getAlias().getName();
						Column c = new Column();
						c.setColumnName(columnName);
						c.setTable(new Table("C" + count));
						eq.setRightExpression(c);		
						if(newExp == null)
							newExp = eq;
						else
							newExp = new AndExpression(newExp, eq);
					}

					pe.setJoinExpressions(newExp);
					parserExpressions.add(pe);
				}

				

				count++;
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
						
						selectItems = new ArrayList<>();
						for(SelectItem<?> item : plainSelect.getSelectItems()){
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
			if(select instanceof ParenthesedSelect){
				ParenthesedSelect parenthesedSelect = (ParenthesedSelect) select;
				if(parenthesedSelect.getSelect() instanceof PlainSelect){

					List<Table> tables = new ArrayList<>();
					
					ParserHelper ph = new ParserHelper();
					PlainSelect plainSelect = (PlainSelect) parenthesedSelect.getSelect();
	
					List<SelectItem<?>> originalColumns = plainSelect.getSelectItems();

					if(plainSelect.getWhere() != null){
						WhereVisitor whereVisitor = new WhereVisitor();
						plainSelect.getWhere().accept(whereVisitor);
						if(whereVisitor.getParserExpressions().size() > 0)
							plainSelect = whereVisitor.processingOperators(plainSelect, whereVisitor);
					}
	
					tables = ph.extractJoinTables(plainSelect);

					JoinConditionsNested joinConditionsNested = new JoinConditionsNested(tables, "C" + count, false);
					joinConditionsNested.splitConditionsWithOperators(plainSelect.getWhere(), "");
					
					plainSelect.setWhere(joinConditionsNested.getWhereExpression());
	
					plainSelect = ph.transformJoins(plainSelect);
	
					for(Column c : joinConditionsNested.getColumns())
						plainSelect.addSelectItems(c);
	
					// Create a new SubSelect
					ParenthesedSelect newSubSelect = new ParenthesedSelect();
	
					// Set the PlainSelect as the subquery
					newSubSelect.setSelect(plainSelect);
	
					newSubSelect.setAlias(new Alias("C" + count));
					
					ExpressionList<Column> groupByList = new ExpressionList<>();
					GroupByElement newGroupByElement = new GroupByElement();
					for(SelectItem<?> si : plainSelect.getSelectItems()){
						if(si.getExpression() instanceof Column){
							groupByList.add((Column) si.getExpression());
						}
					}
					if(groupByList.size() > 0){
						newGroupByElement.setGroupByExpressions(groupByList);
						plainSelect.setGroupByElement(newGroupByElement);	
					}

					ParenthesedSelect subSelect = new ParenthesedSelect();
					subSelect.setSelect(plainSelect);
					subSelect.setAlias(new Alias("C" + count));
	
					ParserExpression pe = new ParserExpression();
					pe.setSelect(subSelect);
					ExpressionList<Expression> joinExpressions = new ExpressionList<Expression>();

					if(havingExists || joinConditionsNested.getExpJoins() == null){
						joinExpressions.add(_newJoin);
						pe.setJoinExpressions(joinExpressions);
					}else{
						joinExpressions.add(new AndExpression(joinConditionsNested.getExpJoins(), _newJoin));
						
						pe.setJoinExpressions(joinExpressions);
					}
					

					if(currentOperator != null && currentOperator.equals("OR"))
					{
						Expression newWhere = null;
						for(SelectItem<?> item : originalColumns){
							IsNullExpression isNotNullExpression = new IsNullExpression();
							Column newColumn = new Column();							
							if(item.getExpression() instanceof Column){
								
								newColumn.setColumnName(((Column)item.getExpression()).getColumnName());
								newColumn.setTable(new Table("C" + count));
							}else{
								if(item.getAlias() != null){
									newColumn.setColumnName(item.getAlias().getName());
									newColumn.setTable(new Table("C" + count));
								}
							}
							isNotNullExpression.setLeftExpression(newColumn);
							isNotNullExpression.setNot(true); // Indicate NOT NULL
							
							if(newWhere == null)
								newWhere = isNotNullExpression;
							else
								newWhere = new AndExpression(newWhere, isNotNullExpression);
						}
						pe.addWhereExpression(newWhere);
					}
	
					parserExpressions.add(pe);
					count++;
					
					/* 
					pe.setSelect(newSubSelect);
					//pe.setJoinTable(tables.getFirst());
					if(yael && havingExists){
						BinaryExpression havingExp = (BinaryExpression)joinExp.get(0);
						
						Column havingCol = (Column) havingExp.getLeftExpression();
						pe.setJoinTable(havingCol.getTable());
						pe.setWhereExpressions(joinExp);
					}else{
						pe.setJoinTable(null);
						pe.setWhereExpressions(joinExp);
					
					}
					parserExpressions.add(pe);
					*/
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
						_columns.add(column);
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
						Column leftCol = (Column) greaterThan.getLeftExpression();
						if(leftCol.getTable().getName().toLowerCase().contains("subhaving"))
							havingExists = true;
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
						String newColumnName = "";
						int func = 0;
						if(plainSelect.getSelectItems().get(0).getExpression() instanceof Column){
							newColumnName = plainSelect.getSelectItem(0).getAlias() == null ? ((Column) plainSelect.getSelectItems().get(0).getExpression()).getColumnName() : plainSelect.getSelectItem(0).getAlias().getName();
						}else if(plainSelect.getSelectItems().get(0).getExpression() instanceof Function){
							if(plainSelect.getSelectItem(0).getAlias() == null)
								plainSelect.getSelectItem(0).setAlias(new Alias("F"+func));
							newColumnName = plainSelect.getSelectItem(0).getAlias().getName();
							func ++;
						}else{
							if(plainSelect.getSelectItem(0).getAlias() == null)
								plainSelect.getSelectItem(0).setAlias(new Alias("F"+func));
							newColumnName = plainSelect.getSelectItem(0).getAlias().getName();
							func ++;
						}
						
						_newJoin = new MinorThan();
						((BinaryExpression) _newJoin).setLeftExpression(minorThan.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), newColumnName));
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
		public <S> Void visit(LikeExpression likeExpression, S context){
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
				boolean functionExists = false;
				if(equalsTo.getRightExpression() instanceof ParenthesedSelect){
					plainSelect = (PlainSelect) ((ParenthesedSelect) equalsTo.getRightExpression()).getSelect();
					
					if(equalsTo.getLeftExpression() instanceof Column){
						String newColumnName = "";
						int func = 0;
						if(plainSelect.getSelectItems().get(0).getExpression() instanceof Column){
							newColumnName = plainSelect.getSelectItem(0).getAlias() == null ? ((Column) plainSelect.getSelectItems().get(0).getExpression()).getColumnName() : plainSelect.getSelectItem(0).getAlias().getName();
						}else if(plainSelect.getSelectItems().get(0).getExpression() instanceof Function){
							if(plainSelect.getSelectItem(0).getAlias() == null)
								plainSelect.getSelectItem(0).setAlias(new Alias("F"+func));
							newColumnName = plainSelect.getSelectItem(0).getAlias().getName();
							func ++;
							functionExists = true;
						}
						Column leftCol = (Column) equalsTo.getLeftExpression();
						if(leftCol.getTable().getName().toLowerCase().contains("subhaving"))
							havingExists = true;

						currentOperator = whereExpressions.get(equalsTo);
						
						_newJoin = new EqualsTo();
						((BinaryExpression) _newJoin).setLeftExpression(equalsTo.getLeftExpression());
						((BinaryExpression) _newJoin).setRightExpression(new Column(new Table("C"+count), newColumnName));
						
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
			}
			super.visit(equalsTo, context);	
			return null;
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
			super.visit(andExpression, context);
			return null;
		}
		
		@Override
		public <S> Void visit(OrExpression orExpression, S context) {
			super.visit(orExpression, context);
			return null;
		}

		public List<ParserExpression> getParserExpressions() {
			return parserExpressions.reversed();
		}

		public Expression getNewWhere() {
			return newWhere;
		}

		public void splitConditionsWithOperators(Expression expression,String currentOperator)
		{
			if (expression instanceof AndExpression) {
				AndExpression andExpression = (AndExpression) expression;
		
				// Recursively process left and right expressions with the "AND" operator
				splitConditionsWithOperators(andExpression.getLeftExpression(),currentOperator.equals("OR") ? "OR" : "AND");
				splitConditionsWithOperators(andExpression.getRightExpression(), "AND");
		
			} else if (expression instanceof OrExpression) {
				OrExpression orExpression = (OrExpression) expression;
		
				// Recursively process left and right expressions with the "OR" operator
				splitConditionsWithOperators(orExpression.getLeftExpression(), "OR");
				splitConditionsWithOperators(orExpression.getRightExpression(), "OR");
		
			} else {
				whereExpressions.put(expression, currentOperator);
			}
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
		private boolean exists = false;

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
			tableJoin = col1.getTable();
			if (exists) col1.setTable(new Table("nestedT"+count)); else col2.setTable(new Table(isAll ? "AllT"+count : "C"+count));


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