package com.Parser;

import java.util.List;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.Alias.AliasColumn;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.UnionOp;
import net.sf.jsqlparser.statement.select.WithItem;

import com.Helper.AggColumns;
import com.Helper.ListAggColumns;
import com.Helper.ParserHelper;
import com.Parser.ParserVisitor.*;
/** 
 * This class is responsible for parsing the SQL query
 * 
 * @author Paulo Pintor
 * @version 1.0
 * @since 1.0
*/
public class Parser {
    private String dbname = "";
	private boolean withTbInfo = false;
	private String lastFunc = "";
	private boolean coalesce = false;
	private boolean originalResult = false;
	private String havingValue = "";
	private Function havingColumn = null;
	/**
	 * The class constructor
	 * 
	 */
	public Parser(String dbname, boolean withTbInfo, boolean originalResult) {
		this.dbname = dbname;
		this.withTbInfo = withTbInfo;
		this.originalResult = originalResult;
	}

	/**
	 * A function which receives a query and returns the same query with the annotations
	 * @param query the query to be parsed
	 * @return the query with the annotations
	 * @throws Exception
	 */
	public String rewriteQuery(String query) throws Exception{
		Statement statement = CCJSqlParserUtil.parse(query);
		String result = "";

		if (statement instanceof Select) {
			Select select = (Select) statement;
			
			select = addAnnotations(select, false);
			result = select.toString();
		}

		return result;
	}

	/**
	 * The function responsible to detect the type of query and help on the algorithm recursion
	 * @param object the query as an object
	 * @return a PlainSelect object with the annotations
	 * @throws Exception
	 */
	private PlainSelect addAnnotations(Object object, boolean isSubSelect) throws Exception {
		PlainSelect newSelect = null;

		if(object instanceof SetOperationList)
		{
			SetOperationList setOperationList = (SetOperationList) object;
		
			List<SetOperation> temp = setOperationList.getOperations();

			boolean union = false;

			for(SetOperation op : temp){
				if(op instanceof UnionOp){
					union = true;
				}else{
					union = false;
					break;
				}
			}

			if(union){
				newSelect = UnionF(setOperationList);
			}else{
				throw new InvalidParserOperation();
			}
		}else if(object instanceof ParenthesedSelect){
			ParenthesedSelect parenthesedSelect = (ParenthesedSelect) object;
			//isAll = parenthesedSelect.getAlias() != null && parenthesedSelect.getAlias().getName().contains("AllT");
			newSelect = addAnnotations(parenthesedSelect.getSelect(), true);
			parenthesedSelect.setSelect(newSelect);
		}else{
			PlainSelect plainSelect = (PlainSelect) object;
			
			if(plainSelect.getHaving() != null){
				plainSelect	= CheckHaving(plainSelect);
			}

			if(plainSelect.getWhere() != null){
				plainSelect = CheckWhere(plainSelect);
			}

			ParserHelper pHelper = new ParserHelper();
			pHelper.transformJoins(plainSelect);

			if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()){
				newSelect = JoinF(plainSelect, isSubSelect);
			}else{
				newSelect = ProjectionSelectionF(plainSelect, isSubSelect);
			}

			filterWhere(newSelect);
			boolean hasFuncGroupBy = pHelper.projectionOnlyFunc(newSelect.getSelectItems());

			if(newSelect.getGroupBy() != null || hasFuncGroupBy)
			{
				newSelect = GroupByF(newSelect, isSubSelect);
			}else if(newSelect.getDistinct() != null){
				newSelect = DistinctF(newSelect);
			}
			havingColumn = null;
			havingValue = "";
		}

		List<WithItem> withItemsList = newSelect.getWithItemsList();

		if(withItemsList != null && !withItemsList.isEmpty()){
			for(WithItem withItem : withItemsList){
				PlainSelect withSelect = addAnnotations(withItem.getSelect(), false);
				ParenthesedSelect parenthesedSelect = new ParenthesedSelect();
				parenthesedSelect.setSelect(withSelect);
				withItem.setSelect(parenthesedSelect);
			}
		}
		coalesce = false;
		return newSelect;
	}

	/**
	 * A function which deals with the selection and projection queries
	 * 
	 * @param plainSelect the query as a PlainSelect object
	 * @return a PlainSelect object with the annotations
	 * @throws Exception
	 */
	private PlainSelect ProjectionSelectionF(PlainSelect plainSelect, boolean isSubSelect) throws Exception {
		String prov = "";
		boolean functionSubquery = false;
		ParserHelper pHelper = new ParserHelper();
		lastFunc = "ProjectionSelectionF";
		if(plainSelect.getFromItem() instanceof Table){
			Table tempTable = (Table) plainSelect.getFromItem();
			
			if(withTbInfo)
				prov = "CONCAT('"+tempTable.getFullyQualifiedName().replace('.',':') + ":', " + (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName()) + ".prov)";
			else
				prov = (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName()) + ".prov";
		
		}else{
			ParenthesedSelect tempSubSelect = (ParenthesedSelect) plainSelect.getFromItem();
			if(tempSubSelect.getAlias().getAliasColumns() != null){
				int i = 0;
				for(AliasColumn column : tempSubSelect.getAlias().getAliasColumns()){
					tempSubSelect.getPlainSelect().getSelectItems().get(i).setAlias(new Alias(column.name));
					i++;
				}
			}
			PlainSelect _plainSelect = addAnnotations(tempSubSelect.getSelect(), true);



			tempSubSelect.setSelect(_plainSelect);
			String aggProv = "";
			
			if(_plainSelect.getSelectItems().get(0).getExpression() instanceof Column && ((Column)_plainSelect.getSelectItems().get(0).getExpression()).getFullyQualifiedName().contains("_un")){
				for(SelectItem<?> item : _plainSelect.getSelectItems()){
					Column column = (Column) item.getExpression();
					if(item.getAlias() != null && item.getAlias().getName().contains("2"))
						aggProv += "|| COALESCE(' * (' || "+tempSubSelect.getAlias().getName()+"."+item.getAlias().getName()+", '') || ')'";
					else if(column.getColumnName().contains("2")){
						aggProv += "|| COALESCE(' * (' || "+tempSubSelect.getAlias().getName()+"."+column.getColumnName()+", '') || ')'";
					}
				}
			}else{
				
				List<String> aggCols = pHelper.getAggColumns(((PlainSelect)tempSubSelect.getSelect()).getSelectItems());
				if(!aggCols.isEmpty()){					
					List<SelectItem<?>> _selectItems = new ArrayList<>();
					for(SelectItem<?> selectItem : plainSelect.getSelectItems()){
						if(selectItem.getExpression() instanceof Column){
							Column column = (Column) selectItem.getExpression();
							if(aggCols.contains(column.getColumnName())){
								Column newColumn = new Column(column.getColumnName()+"2");
								newColumn.setTable(new Table(tempSubSelect.getAlias().getName()));
								SelectItem<?> newSelectItem = new SelectItem<>(newColumn);
								if(!isSubSelect){
									newSelectItem.setAlias(new Alias((selectItem.getAlias() != null ? selectItem.getAlias().getName(): column.getColumnName())));
								}else{
									newSelectItem.setAlias(new Alias((selectItem.getAlias() != null ? selectItem.getAlias().getName() +"2": column.getColumnName()+"2")));
								}
								_selectItems.add(newSelectItem);
							}else{
								_selectItems.add(selectItem);
							}

						}else if(selectItem.getExpression() instanceof Function){
							Function function = (Function) selectItem.getExpression();
							ExpressionList<?> parameters = function.getParameters();
							if(parameters.size() == 1 && parameters.get(0) instanceof Column){
								Column column = (Column) parameters.get(0);
								if(aggCols.contains(column.getColumnName())){

									String separator = "+"+function.getName().toLowerCase();
									String aggCol = pHelper.getAggFunction("CONCAT("+column.getColumnName()+"2" + ", ' "+(char) 0x2297+" ')", separator, (plainSelect.getSelectItems().get(0).getExpression() instanceof Column ? ((Column) plainSelect.getSelectItems().get(0).getExpression()).getFullyQualifiedName() : "1"), dbname);
									Column newColumn = new Column(aggCol);
									SelectItem<?> newSelectItem = new SelectItem<>(newColumn);
									
									if(!isSubSelect){
										newSelectItem.setAlias(new Alias((selectItem.getAlias() != null ? selectItem.getAlias().getName(): column.getColumnName())));
										
									}else{
										newSelectItem.setAlias(new Alias((selectItem.getAlias() != null ? selectItem.getAlias().getName() +"2": column.getColumnName()+"2")));
									
									}
									_selectItems.add(newSelectItem);
									aggProv += "|| ' * (' || "+tempSubSelect.getAlias().getName()+'.'+column.getColumnName()+"2 || ')'";

									functionSubquery = true;
								}

							}else{
								_selectItems.add(selectItem);
							}


						}else{
							_selectItems.add(selectItem);

						}
					}

					plainSelect.setSelectItems(_selectItems);
				}

			}

			if (tempSubSelect.getAlias() == null){
				prov = "prov" + aggProv; 
			}else
				prov = tempSubSelect.getAlias().getName()+".prov  " + aggProv;		
				
			if(functionSubquery){
				prov = "CONCAT('δ('," + pHelper.getAggFunction(prov, "+", (plainSelect.getSelectItems().get(0).getExpression() instanceof Column && !((Column) plainSelect.getSelectItems().get(0).getExpression()).getFullyQualifiedName().toLowerCase().contains("agg") ? ((Column) plainSelect.getSelectItems().get(0).getExpression()).getFullyQualifiedName() : ""), dbname) + ",')')";
			}
		}
		
		SelectItem<Column> newColumn = new SelectItem<Column>();
		
		// Create a new SelectExpressionItem for the new column
		newColumn.setExpression(new Column(prov));
		newColumn.setAlias(new Alias("prov"));
		
		plainSelect.addSelectItems(newColumn);
		
		return plainSelect;
	}

	/**
	 * A function which deals with the queries with JOINS
	 * 
	 * @param plainSelect the query as a PlainSelect object
	 * @param correlated a boolean to indicate if the query is correlated or not
	 * @return a PlainSelect object with the annotations
	 * @throws Exception
	 */
	private PlainSelect JoinF(PlainSelect plainSelect, boolean isSubSelect) throws Exception {
		String provToken = "";
		//ParserHelper pHelper = new ParserHelper();
		lastFunc = "JoinF";
		coalesce = false;
		boolean having = false;
		String havingColumn = "";
        if(plainSelect.getFromItem() instanceof Table)
        {
            Table tempTable = (Table) plainSelect.getFromItem();

			if(withTbInfo)
           		provToken = "'" + tempTable.getFullyQualifiedName().replace('.',':') + ":' || " + (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
			else
				provToken = (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
				//provToken = (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
        }else{
			ParenthesedSelect tempSubSelect = (ParenthesedSelect) plainSelect.getFromItem();
			PlainSelect newPlainSelect = (PlainSelect) tempSubSelect.getSelect();
			tempSubSelect.setSelect(addAnnotations(newPlainSelect, true));
			
			/*if (tempSubSelect.getAlias().getName().contains("Having")){
				for(SelectItem<?> item : newPlainSelect.getSelectItems()){
					if(item.getAlias() != null && item.getAlias().getName().contains("_agg")){
						Column _havingCol = null;
						if(!isSubSelect){
							String alias = item.getAlias().getName().replace("_agg", "");
							plainSelect.getSelectItems().removeIf(col -> ((Column)col.getExpression()).getColumnName().equals(alias));
							_havingCol = new Column(new Table(tempSubSelect.getAlias().getName()), item.getAlias().getName());
							SelectItem<Column> newColumn = new SelectItem<Column>(_havingCol);
							newColumn.setAlias(new Alias(alias));
							plainSelect.addSelectItems(newColumn);
						}else{
							_havingCol = new Column(new Table(tempSubSelect.getAlias().getName()), item.getAlias().getName());
							plainSelect.addSelectItems(new SelectItem<Column>(_havingCol));
						}

						
						if(plainSelect.getJoin(0).isLeft()){
							Expression joinClause = (Expression)plainSelect.getJoin(0).getOnExpressions().toArray()[0];
							if(joinClause instanceof GreaterThan){
								GreaterThan gt = (GreaterThan) joinClause;
									if(gt.getLeftExpression() instanceof Column){
										Column col = (Column) gt.getLeftExpression();
										if(col.getTable().getName().contains("Having")){
											havingColumn = _havingCol.toString() + " || ' > ' || COALESCE( '1 . (' || "+gt.getRightExpression().toString()+"2 || ')', ' 1 . 0')";
										}
									}
							}
						}else{
							Expression where = plainSelect.getWhere();
							if(where != null){
								if(where instanceof EqualsTo){
									EqualsTo eq = (EqualsTo) where;
									if(eq.getLeftExpression() instanceof Column){
										Column col = (Column) eq.getLeftExpression();
										if(col.getTable().getName().contains("Having")){
											havingColumn = eq.getRightExpression().toString();
										}
									}
								}else if(where instanceof GreaterThan){
									GreaterThan gt = (GreaterThan) where;
									if(gt.getLeftExpression() instanceof Column){
										Column col = (Column) gt.getLeftExpression();
										if(col.getTable().getName().contains("Having")){
											havingColumn = _havingCol.toString() + " || ' > ' || '1 . (' "+gt.getRightExpression().toString()+"2 || ')";
										}
									}
								}
							}
						}
					}
				}
				provToken = tempSubSelect.getAlias().getName()+".prov";
				having = true;
			}else*/
			//provToken = "'('||" + tempSubSelect.getAlias().getName()+".prov " + "|| ')'";
			provToken = tempSubSelect.getAlias().getName()+".prov";
		}

		for(Join join : plainSelect.getJoins()) {
			if(join.getRightItem() instanceof Table){            
                Table tempTable = (Table) join.getRightItem();
				if(tempTable.getAlias() != null && tempTable.getAlias().getName().contains("nestedT")){
					continue;
				}else{
					if(join.isLeft()){
						String leftProv;
						if(withTbInfo) 
							leftProv = "'" + tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov";
						else
							leftProv = (tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov";

						//provToken = "COALESCE('(' || " + provToken + " || ' "+(char) 0x2297+" ' || "+leftProv+" || ')', '(' ||"+provToken+"|| ')')";
						provToken = "COALESCE("+provToken + " || ' . ' || "+leftProv+" , "+provToken+")";
						coalesce = true;
					}else if(join.isRight()){
						String rightProv;
						
						if(withTbInfo)
							rightProv = "'"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
						else
							rightProv = (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";

						//provToken = "COALESCE('(' || " + provToken + " || ' "+(char) 0x2297+" ' || "+rightProv+" || ')', '( ' || "+rightProv+") || ')')";
						provToken = "COALESCE(" + provToken + " || ' . ' || "+rightProv+", "+rightProv+")";
						coalesce = true;
					}else{
						if(withTbInfo)
							provToken = provToken + " || ' . ' || '"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
						else
							provToken = provToken + " || ' . ' || " +(tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
					}
				}
            }else{
				ParenthesedSelect tempSubSelect = (ParenthesedSelect) join.getRightItem();

				if(tempSubSelect.getAlias() != null && tempSubSelect.getAlias().getName().contains("nestedT")){
					continue;
				}else
					tempSubSelect.setSelect(addAnnotations(tempSubSelect, true));

				if(join.isLeft()){
					if(having){
						String aggProv = "|| ' . [' || "+havingColumn+" || ']'";

						provToken = provToken + " || COALESCE(' . ' || "+tempSubSelect.getAlias().getName()+".prov, '')"+aggProv;
						//provToken = provToken + "|| ' "+(char) 0x2297+" ' || "+tempSubSelect.getAlias().getName()+".prov";
					}else{
						//provToken = "COALESCE('(' || " + provToken + " || ' "+(char) 0x2297+" ' || "+tempSubSelect.getAlias().getName()+".prov || ')', '(' || "+provToken+"|| ')')";
						provToken = provToken + " || COALESCE(' . ' || '(' || "+tempSubSelect.getAlias().getName()+".prov || ')', '')";
						
						coalesce = true;	
					}
				}
				else if(join.isRight()){
					//provToken = "COALESCE('(' || " + provToken + " || ' "+(char) 0x2297+" ' || "+tempSubSelect.getAlias().getName()+".prov|| ')', '(' || "+tempSubSelect.getAlias().getName()+".prov || ')')";
					provToken = "COALESCE(" + provToken + " || ' . ' || "+tempSubSelect.getAlias().getName()+".prov, "+tempSubSelect.getAlias().getName()+".prov)";
					
					coalesce = true;
				}
				else{
					/*if(having){
						String aggProv = "|| ' . [' || "+havingColumn+" || ']'";

						provToken = "'δ(' || "+provToken + " || ' . ' || "+tempSubSelect.getAlias().getName()+".prov || ')'"+aggProv;
						
					}else{*/
						JoinOnVisitor joinOnVisitor = new JoinOnVisitor(originalResult, plainSelect);
						//join.getOnExpressions().forEach(exp -> exp.accept(joinOnVisitor));
						List<Expression> joinExpression = new ArrayList<>();
						for(Expression exp : join.getOnExpressions()){
							exp.accept(joinOnVisitor);
							if(!originalResult){
								Expression newJExpression = joinOnVisitor.removeOneOne(exp);
								if(newJExpression != null) joinExpression.add(newJExpression);
							}
						}

						if(!originalResult){
							if(joinExpression.size() == 0){
								join.setOnExpressions(joinExpression);
								join.setCross(true);
								join.setInner(false); 
							}else{
								join.setOnExpressions(joinExpression);
							}
						}
						
						 
						/* 
						List<FromItem> fromItems = new ArrayList<>();
						fromItems.add(plainSelect.getFromItem());
						for(Join _join : plainSelect.getJoins()){
							fromItems.add(_join.getRightItem());
						}
						String aggProv = pHelper.aggJoin(fromItems, join.getOnExpressions(), tempSubSelect);
						*/
						provToken = provToken + " || ' . ' || '(' || "+tempSubSelect.getAlias().getName()+".prov || ')'"+(joinOnVisitor.getSymbolicExpression().equals("") ? "" : "||" + joinOnVisitor.getSymbolicExpression() );
					//}
				}

			}
			
		}
		
		SelectItem<Column> newColumn = new SelectItem<Column>();
		newColumn.setExpression(new Column(provToken));
		
		newColumn.setAlias(new Alias("prov"));
		plainSelect.addSelectItems(newColumn);
		
		return plainSelect;
	}

	/**
	 * A function which deals with the queries with UNION
	 * @param setOperationList the query as a SetOperationList object containing all the UNION queries
	 * @return a PlainSelect object with the annotations
	 * @throws Exception
	 */
	private PlainSelect UnionF(SetOperationList setOperationList) throws Exception {
		ParserHelper pHelper = new ParserHelper();
		boolean unionAll = true;
		lastFunc = "UnionF";
		List<SelectItem<?>> selectItems = new ArrayList<>();
		selectItems = pHelper.getUnionColumns(setOperationList.getSelects().get(0));

		List<Integer> funcIndexes = new ArrayList<>();
		for(Select select : setOperationList.getSelects()){

			if(!originalResult){
				if (select instanceof PlainSelect){
					int i = 0;
					for(SelectItem<?> selectItem : ((PlainSelect) select).getSelectItems()){
						if(selectItem.getExpression() instanceof Function){
							Function function = (Function) selectItem.getExpression();
							if(function.getName().toLowerCase().equals("sum") || function.getName().toLowerCase().equals("avg") || function.getName().toLowerCase().equals("count") || function.getName().toLowerCase().equals("max") || function.getName().toLowerCase().equals("min")){
								funcIndexes.add(i);
							}
						}
						i++;
					}
				}
			}

			addAnnotations(select, false);
		}

		if(!originalResult){
			for(Select select : setOperationList.getSelects()){
				List<SelectItem<?>> items = ((PlainSelect) select).getSelectItems();
				for(int j : funcIndexes){
					if(items.get(j).getExpression() instanceof Column && !items.get(j).getExpression().toString().contains("prov")){
						Column castColumn = (Column) items.get(j).getExpression();

						CastExpression cast = new CastExpression();
						cast.setLeftExpression(items.get(j).getExpression());
						ColDataType colDataType = new ColDataType();
						colDataType.setDataType("VARCHAR");
						cast.setColDataType(colDataType);
						SelectItem<?> selectItem = new SelectItem<>(cast);
						Alias castAlias = items.get(j).getAlias() != null ? items.get(j).getAlias() : new Alias(castColumn.getColumnName());
						selectItem.setAlias(castAlias);
						
						items.set(j, selectItem);
					}
				}
			}
		}

		
		for(SetOperation op : setOperationList.getOperations()){
			UnionOp union = (UnionOp) op;
			if(union.isAll() == false){
				union.setAll(true);
				unionAll = false;
			}
		}

		PlainSelect newSelect = new PlainSelect();
		if (setOperationList.getWithItemsList() != null) {
			newSelect.setWithItemsList(setOperationList.getWithItemsList());
			setOperationList.setWithItemsList(null);
			
		}

		
		ParenthesedSelect parenthesedSelect = new ParenthesedSelect();
		parenthesedSelect.setSelect(setOperationList);
		parenthesedSelect.setAlias(new Alias("_un"));
		newSelect.setFromItem(parenthesedSelect);

		if(!unionAll){
			ExpressionList<Column> groupByList = new ExpressionList<Column>();
			for(SelectItem<?> ex : selectItems){
				groupByList.addExpression((Column) ex.getExpression());
			}
			
			GroupByElement groupBy = new GroupByElement();
			groupBy.setGroupByExpressions(groupByList);
			newSelect.setGroupByElement(groupBy);		
			
		}

		List<SelectItem<?>> unionList = getFirstPlainSelectColumns(setOperationList.getSelects().get(0));
		List<SelectItem<?>> newSelectItems = new ArrayList<>();
		for(SelectItem<?> selectItem : unionList){
			if(selectItem.getExpression() instanceof Column){
				if(selectItem.getAlias() != null){
					SelectItem<Column> newColumn = new SelectItem<Column>(new Column(new Table("_un"),selectItem.getAlias().getName()));
					newSelectItems.add(newColumn);
				}else{
					Column column = (Column) selectItem.getExpression();
					SelectItem<Column> newColumn = new SelectItem<Column>(new Column(new Table("_un"),column.getColumnName()));
					newSelectItems.add(newColumn);
				}
			}else if(selectItem.getExpression() instanceof Function){
				if(selectItem.getAlias() != null){
					SelectItem<Column> newColumn = new SelectItem<Column>(new Column(new Table("_un"),selectItem.getAlias().getName()));
					newSelectItems.add(newColumn);
				}else{
					//TODO: If the function has no alias
				}
			}
		}
		/*
		Column prov = new Column();
		prov.setColumnName("prov");
		prov.setTable(new Table("_un"));

		SelectItem<Column> provItem  = new SelectItem<Column>(prov);
		provItem.setAlias(new Alias("prov"));
		selectItems.add(provItem);
		 */
		newSelect.setSelectItems(newSelectItems);
		return newSelect;		
	}	

	/**
	 * A function which deals with the queries with GROUP BY clause
	 * 
	 * @param newSelect the query as a PlainSelect object
	 * @return a PlainSelect object with the annotations
	 * @throws Exception
	 */
	private PlainSelect GroupByF(PlainSelect newSelect, boolean isSubSelect) throws Exception {
		ParserHelper pHelper = new ParserHelper();		
		String separator = "";
		FunctionProjection funcVisitor = new FunctionProjection();
		funcVisitor.setCoalesce(coalesce);
		newSelect.getSelectItems().forEach(item -> item.accept(funcVisitor, null));
		//if(funcVisitor.hasFunction() && !isAll) aggFunction = funcVisitor.getAggExpression();

		String firstColumn = pHelper.aggFunctionOrderBy(newSelect.getSelectItems().get(0));

		List<SelectItem<?>> newSelectItems = new ArrayList<>();
		
		ListAggColumns aggColumns = funcVisitor.getAggColumns();

		for (SelectItem<?> selectItem : newSelect.getSelectItems()) {
			if (selectItem.getExpression() instanceof Column) {
				if(selectItem.getAlias() != null && selectItem.getAlias().getName() == "prov" || ((Column)selectItem.getExpression()).getColumnName().contains("prov"))
				{	
					String aggCol = "";
					Column column = (Column) selectItem.getExpression();
					if(column.getTable() != null && column.getTable().getName().contains("_un")){
						String newColumnStr = "";
						for (SelectItem<?> _selectItem : newSelect.getSelectItems()) {
							if (_selectItem.getExpression() instanceof Column && ((Column)_selectItem.getExpression()).getFullyQualifiedName().contains("_agg")) {
								newSelectItems.remove(_selectItem);
								newColumnStr = pHelper.getAggFunction(((Column)_selectItem.getExpression()).getColumnName(), "+", firstColumn, dbname);
								SelectItem<Column> newColumn = new SelectItem<Column>(new Column(newColumnStr));
								newColumn.setAlias(new Alias(((Column)_selectItem.getExpression()).getColumnName()));
								newSelectItems.add(newColumn);
							}
						}

						
						//The need of the replace is for the Monoids to be possible to obtain the right result in AVGs or SUM / SUM or NUMBER / COUNT
						newColumnStr = pHelper.getAggFunction(column.toString(), "+", firstColumn, dbname);
						SelectItem<Column> newColumn = new SelectItem<Column>(new Column(newColumnStr));
						newColumn.setAlias(new Alias("prov"));
						newSelectItems.add(newColumn);		
						
					}else{
						StringBuilder output = new StringBuilder(column.toString());
						String startDelimiter = "|| '. [ '";
						String endDelimiter = "' ]'";
						String newProvColumn = column.toString();
						if (column.toString().contains(startDelimiter) || column.toString().contains(endDelimiter)) {
							int startIndex = output.indexOf(startDelimiter);
							while (startIndex != -1) {
								int endIndex = output.indexOf(endDelimiter, startIndex);
								if (endIndex != -1) {
									// Remove from startDelimiter to end of endDelimiter
									output.delete(startIndex, endIndex + endDelimiter.length());
								} else {
									// If no matching endDelimiter is found, stop the loop
									break;
								}
								startIndex = output.indexOf(startDelimiter); // Find next occurrence
							}
							newProvColumn = output.toString();
						}

						 
						if(aggColumns.getAggColumns().size() == 0 && havingColumn != null){
							if(havingColumn.getName().toLowerCase().equals("sum")){
								separator = "+sum";
								aggCol = pHelper.getAggFunction("CONCAT("+newProvColumn + ", ' "+(char) 0x2297+" ', " + havingColumn.getParameters().toString() +")",separator, firstColumn, dbname);
							}
						}else{
							for(AggColumns aggColumn : aggColumns.getAggColumns()){									
								
								if(aggColumn.getValue().equals("avg")){
									separator = "+avg";
									aggCol = "REPLACE("+pHelper.getAggFunction("CONCAT("+ newProvColumn + ", ' "+(char) 0x2297+" ', " +aggColumn.getKey()+",'/avgt'"+")", separator, firstColumn, dbname)+", 'avgt', CAST(COUNT(*) AS VARCHAR))";
									SelectItem<Column> newColumn = new SelectItem<Column>(new Column(aggCol));
									if(!originalResult) newColumn.setAlias(new Alias(aggColumn.getAlias()));
									else newColumn.setAlias(new Alias(aggColumn.getAlias()+"_agg"));
									newSelectItems.add(newColumn);		
								}else if(aggColumn.getValue().equals("count") && !aggColumn.getKey().equals("*") && coalesce){
									separator = "+count";
									aggCol = pHelper.getAggFunction("CONCAT("+ newProvColumn + ", ' "+(char) 0x2297+" ', " +" (CASE when "+aggColumn.getKey()+" IS NULL then '0' else '1' end))", separator, firstColumn, dbname);
									SelectItem<Column> newColumn = new SelectItem<Column>(new Column(aggCol));
									if(!originalResult) newColumn.setAlias(new Alias(aggColumn.getAlias()));
									else newColumn.setAlias(new Alias(aggColumn.getAlias()+"_agg"));
									newSelectItems.add(newColumn);
								}else{
									separator = aggColumn.getValue().equals("sum") ? "+sum" : (aggColumn.getValue().equals("min") ? "+min" : (aggColumn.getValue().equals("max") ? "+max" : "+count"));
									aggCol = pHelper.getAggFunction("CONCAT('(',"+newProvColumn + ", ')', ' "+(char) 0x2297+" ', " + (aggColumn.getValue().equals("count") ? "'1'" :aggColumn.getKey())+")", separator, firstColumn, dbname);
									SelectItem<Column> newColumn = new SelectItem<Column>(new Column(aggCol));
									if(!originalResult) newColumn.setAlias(new Alias(aggColumn.getAlias()));
									else newColumn.setAlias(new Alias(aggColumn.getAlias()+"_agg"));
									newSelectItems.add(newColumn);		
								}
							}
						}
						String newColumnStr = "";
						if(aggColumns.getAggColumns().size() > 0)
							if(havingColumn != null)
								newColumnStr = "CONCAT('δ('," + pHelper.getAggFunction(column.toString(), "+", firstColumn, dbname) + ",') . [',"+aggCol+" ,'"+havingValue+"]')";
							else 
								newColumnStr = "CONCAT('δ('," + pHelper.getAggFunction(column.toString(), "+", firstColumn, dbname) + ",')')";
						else if(aggColumns.getAggColumns().size() == 0 && havingColumn != null){
							newColumnStr = "CONCAT('δ('," + pHelper.getAggFunction(column.toString(), "+", firstColumn, dbname) + ",') . [',"+aggCol+" ,'"+havingValue+"]')";
						}else
							newColumnStr = pHelper.getAggFunction(column.toString(), "+", firstColumn, dbname);
						SelectItem<Column> newColumn = new SelectItem<Column>(new Column(newColumnStr));
						newColumn.setAlias(new Alias("prov"));
						newSelectItems.add(newColumn);		
					}
				}
				else{
					if(aggColumns.getAggColumns().size() > 0 && !originalResult){
						Column column = (Column) selectItem.getExpression();

						if(!aggColumns.containsColumn(column.getColumnName()) && !column.getColumnName().equals("prov"))
							newSelectItems.add(selectItem);
					}else
						newSelectItems.add(selectItem);
				}
			}
			else{
				if(aggColumns.getAggColumns().size() > 0 && !originalResult){
					if(selectItem.getAlias() != null && !selectItem.getAlias().getName().equals("prov")){
						if(!aggColumns.containsColumn(selectItem.getAlias().getName()))
							newSelectItems.add(selectItem);
					}
				}else{
					newSelectItems.add(selectItem);
				}
			}
		}
		newSelect.setSelectItems(newSelectItems);

		if(newSelect.getSelectItems().get(0).getExpression() instanceof Column){
			Column column = (Column) newSelect.getSelectItems().get(0).getExpression();
			ExpressionList<Column> partition = new ExpressionList<>();
			List<Column> newNestedItems = new ArrayList<>();
			String newNestedTable = "";
			if(column.toString().toLowerCase().contains("nestedt")){
				for(SelectItem<?> cols : newSelect.getSelectItems()){
					if(cols.getExpression() instanceof Column){
						Column col = (Column) cols.getExpression();
						if(col.getTable() != null && col.getTable().getAlias() != null && col.getTable().getAlias().getName().contains("nestedT")){
							partition.add(col);
							Column newCol = new Column();
							newCol.setColumnName(col.getColumnName());
							newNestedItems.add(newCol);
						}
					}
				}
				for(Join j : newSelect.getJoins()){
					if(j.getRightItem() instanceof Table){
						Table table = (Table) j.getRightItem();
						if(table.getAlias().getName().toLowerCase().contains("nestedt")){
							newNestedTable = table.getAlias().getName();
							newSelect.addGroupByColumnReference(new Column(new Table(table.getAlias().getName()), "prov"));
						}
					}else{
						ParenthesedSelect parenthesedSelect = (ParenthesedSelect) j.getRightItem();
						if(parenthesedSelect.getAlias().getName().toLowerCase().contains("nestedt")){
							newSelect.addGroupByColumnReference(new Column(new Table(parenthesedSelect.getAlias().getName()), "prov"));
							newNestedTable = parenthesedSelect.getAlias().getName();
						}
					}
				}

				Function countFunction = new Function();
				countFunction.setName("COUNT");
				countFunction.setParameters(new ExpressionList<>().addExpression(new AllColumns()));
				
				// Assign an alias if needed
				SelectItem<?> newCountAll = new SelectItem<>(countFunction);
				newCountAll.setAlias(new Alias("cnt1"));

				newSelect.addSelectItems(newCountAll);

		
				AnalyticExpression countFunctionPart = new AnalyticExpression();
				countFunctionPart.setExpression(new AllColumns());
				countFunctionPart.setName("COUNT");
				countFunctionPart.setPartitionExpressionList(partition);

				SelectItem<?> newCountPart = new SelectItem<>(countFunctionPart);
				newCountPart.setAlias(new Alias("cnt2"));

				newSelect.addSelectItems(newCountPart);

				PlainSelect surrondSelect = new PlainSelect();
				ParenthesedSelect newFromItem = new ParenthesedSelect();
				newFromItem.setSelect(newSelect);
				surrondSelect.setFromItem(newFromItem);
				newFromItem.setAlias(new Alias(newNestedTable+"_2"));
				for(Column col : newNestedItems){
					surrondSelect.addSelectItems(new SelectItem<>(new Column(new Table(newNestedTable+"_2"), col.getColumnName())));
					surrondSelect.addGroupByColumnReference(new Column(new Table(newNestedTable+"_2"), col.getColumnName()));
				}
				surrondSelect.addGroupByColumnReference(new Column(new Table(newNestedTable+"_2"), "cnt1"));
				surrondSelect.addGroupByColumnReference(new Column(new Table(newNestedTable+"_2"), "cnt2"));
				surrondSelect.addGroupByColumnReference(new Column(new Table(newNestedTable+"_2"), "prov"));
				
				CaseExpression caseExpression = new CaseExpression();
				WhenClause whenClause = new WhenClause();
        		whenClause.setWhenExpression(new MinorThan(new Column(new Table(newNestedTable+"_2"), "cnt1"), new Column(new Table(newNestedTable+"_2"), "cnt2")));
       			whenClause.setThenExpression(new Column(new Table(newNestedTable+"_2"), "prov"));
				caseExpression.addWhenClauses(whenClause);
				caseExpression.setElseExpression(new Column(pHelper.getAggFunction(new Column(new Table(newNestedTable+"_2"), "prov").toString(), "+", ((Column)surrondSelect.getSelectItems().get(0).getExpression()).toString(), dbname)));
				SelectItem<CaseExpression> newCase = new SelectItem<>(caseExpression);
				newCase.setAlias(new Alias("prov"));
				surrondSelect.addSelectItems(newCase);

				return surrondSelect;
				
			}
			
		}

		return newSelect;
	}

	/**
	 * A function which deals with the queries that contain in the Projection a Distinct clause and the apply their rules
	 * @param newSelect the query as a PlainSelect object
	 * @return a PlainSelect object with the changes and the rules of Distinct
	 * @throws Exception
	 */
	private PlainSelect DistinctF(PlainSelect newSelect) throws Exception {
		ParserHelper pHelper = new ParserHelper();		

		ExpressionList<Column> groupByList = new ExpressionList<>();
		
		String firstColumn = pHelper.aggFunctionOrderBy(newSelect.getSelectItems().get(0));
 		
		List<SelectItem<?>> newSelectItems = new ArrayList<>();

		for (SelectItem<?> selectItem : newSelect.getSelectItems()) {
			if(selectItem.getAlias() != null && selectItem.getAlias().getName() == "prov")
			{
				Column column = (Column) selectItem.getExpression();
				SelectItem<Column> newColumn = null;
						
				if(lastFunc == "ProjectionSelectionF")
					newColumn = new SelectItem<Column>(new Column(pHelper.getAggFunction(column.toString(), "+", firstColumn, dbname)));
				else
					newColumn = new SelectItem<Column>(new Column(pHelper.getAggFunction("'(' ||"+column.toString()+"|| ')'", "+", firstColumn, dbname)));
				
				newColumn.setAlias(new Alias("prov"));
				newSelectItems.add(newColumn);
			}else if(selectItem.getExpression() instanceof Column){
				groupByList.addExpression((Column) selectItem.getExpression());	
				newSelectItems.add(selectItem);			
			}
		}
		GroupByElement groupBy = new GroupByElement();
		groupBy.setGroupByExpressions(groupByList);
		newSelect.setGroupByElement(groupBy);
		newSelect.setSelectItems(newSelectItems);
		
		newSelect.setDistinct(null);
		return newSelect;
	}	

	private PlainSelect CheckWhere(PlainSelect plainSelect) {
		WhereVisitor whereVisitor = new WhereVisitor();
		whereVisitor.setOriginalResult(originalResult);
		return whereVisitor.identifyOperators(plainSelect);
	}

	private PlainSelect CheckHaving(PlainSelect plainSelect) {
		HavingVisitor havingVisitor = new HavingVisitor();
		plainSelect.getHaving().accept(havingVisitor, null);

		if(havingVisitor.isRewrite()){
			plainSelect.setHaving(null);
			ParserHelper pHelper = new ParserHelper();
			List<String> colNames = pHelper.havingColumns(havingVisitor.getLeftColumn(),plainSelect.getSelectItems());
			PlainSelect newSelect = new PlainSelect();
			if(colNames.size() > 0){
				
				for (String colName : colNames) {
					Column column = new Column();
					column.setColumnName(colName);
					column.setTable(new Table("subHaving"));
					SelectItem<Column> newColumn = new SelectItem<Column>(column);
					newSelect.addSelectItems(newColumn);
				}
				ParenthesedSelect parenthesedSelect = new ParenthesedSelect();
				parenthesedSelect.setSelect(plainSelect);
				parenthesedSelect.setAlias(new Alias("subHaving"));
				newSelect.setFromItem(parenthesedSelect);
				if(havingVisitor.getOperation().equals("GreaterThan")){
					GreaterThan newCondition = new GreaterThan();
					newCondition.setLeftExpression(new Column(new Table("subHaving"), colNames.getLast()));
					newCondition.setRightExpression((ParenthesedSelect)havingVisitor.getRightColumn());
					newSelect.setWhere(newCondition);
				}
			}

			return newSelect;
		}else{
			if(havingVisitor.getOperation().equals("GreaterThan"))
				havingValue = " > "+havingVisitor.getRightColumn().toString();
			havingColumn = (Function) havingVisitor.getLeftColumn();
			if(originalResult){
				plainSelect.setHaving(null);
			}
		}

		return plainSelect;
	}

	private SetOperationList checkUnionCols(SetOperationList setOperationList){
		int countCols = 0;
		List<SelectItem<?>> selectItems = new ArrayList<>();
		for(Select select : setOperationList.getSelects()){
			if(select instanceof PlainSelect){
				PlainSelect plainSelect = (PlainSelect) select;
				if(countCols < plainSelect.getSelectItems().size()){
					countCols = plainSelect.getSelectItems().size();
					selectItems = plainSelect.getSelectItems();
				}
			}else{
				SetOperationList temp = (SetOperationList) select;
				temp = checkUnionCols(temp);
			}
		}

		for(Select select : setOperationList.getSelects()){
			if(select instanceof PlainSelect){
				PlainSelect plainSelect = (PlainSelect) select;
				if(plainSelect.getSelectItems().size() < countCols){
					List<SelectItem<?>> newSelectItems = new ArrayList<>();
					SelectItem<?> lastCol = plainSelect.getSelectItems().getLast();
					plainSelect.getSelectItems().remove(lastCol);
					for(int i = 0; i < countCols-1; i++){
						if(i < plainSelect.getSelectItems().size()){
							newSelectItems.add(plainSelect.getSelectItems().get(i));
						}else{
							// Add an alias to the NULL value
							Alias alias = new Alias(selectItems.get(i).getAlias().getName());
							SelectItem<?> newSelectItem = new SelectItem<>(new NullValue());
							newSelectItem.setAlias(alias);
							newSelectItems.add(newSelectItem);
						}							
					}
					newSelectItems.add(lastCol);
					plainSelect.setSelectItems(newSelectItems);
				}
			}else{
				SetOperationList temp = (SetOperationList) select;
				temp = checkUnionCols(temp);
			}
		}

		return setOperationList;
	}

	private List<SelectItem<?>> getFirstPlainSelectColumns(Select select) {
        if (select instanceof PlainSelect) {
            return ((PlainSelect) select).getSelectItems();
        } else if (select instanceof SetOperationList) {
            // Get the first SelectBody in the SetOperationList
            SetOperationList setOperationList = (SetOperationList) select;
            if (!setOperationList.getSelects().isEmpty()) {
                return getFirstPlainSelectColumns(setOperationList.getSelects().get(0));
            }
        }
        return null; // No PlainSelect found
    }

	public void filterWhere(PlainSelect plainSelect) {
		if(plainSelect.getWhere() != null){
			JoinOnVisitor joinOnVisitor = new JoinOnVisitor(originalResult, plainSelect);
			plainSelect.getWhere().accept(joinOnVisitor);
			int index = 0;
			Column newColumn = null;
			if(!joinOnVisitor.getSymbolicExpression().equals("")){
				for(int i = 0; i < plainSelect.getSelectItems().size(); i++){
					if(plainSelect.getSelectItems().get(i).getExpression() instanceof Column && plainSelect.getSelectItems().get(i).getAlias() != null && plainSelect.getSelectItems().get(i).getAlias().getName().contains("prov")){
						Column colTemp = (Column) plainSelect.getSelectItems().get(i).getExpression();
						String newColStr = colTemp.toString() + " || " + joinOnVisitor.getSymbolicExpression();
						newColumn = new Column(newColStr);
						index = i;
						break;
					}
				}

				plainSelect.getSelectItems().remove(index);
				SelectItem<Column> newSelectItem = new SelectItem<Column>(newColumn);
				newSelectItem.setAlias(new Alias("prov"));
				plainSelect.addSelectItems(newSelectItem);

				if(!originalResult){
					Expression newWhere = joinOnVisitor.removeOneOne(plainSelect.getWhere());
					plainSelect.setWhere(newWhere);
				}
			}
			

		}
	}
}