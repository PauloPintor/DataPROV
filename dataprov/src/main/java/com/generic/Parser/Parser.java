package com.generic.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.UnionOp;

import com.generic.Parser.ParserVisitors.*;

/** 
 * This class is responsible for parsing the SQL query
 * 
 * @author Paulo Pintor
 * @version 1.0
 * @since 1.0
*/
public class Parser {

	private String dbname = "";

	/**
	 * The class constructor
	 * 
	 */
	public Parser(String dbname){
		this.dbname = dbname;

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
			select.setSelectBody(addAnnotations(select.getSelectBody()));
			result = select.getSelectBody().toString();
		}

		return result;
	}

	/**
	 * The function responsible to detect the type of query and help on the algorithm recursion
	 * @param object the query as an object
	 * @return a PlainSelect object with the annotations
	 * @throws Exception
	 */
	private PlainSelect addAnnotations(Object object) throws Exception {
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
		}else{
			PlainSelect plainSelect = (PlainSelect) object;

			
			if(plainSelect.getWhere() != null){
				CheckWhere(plainSelect);
			}
			 

			if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()){
				newSelect = JoinF(plainSelect, false);
			}else{
				newSelect = ProjectionSelectionF(plainSelect);
			}
			
		}

		ParserHelper pHelper = new ParserHelper();
		boolean hasFuncGroupBy = pHelper.projectionOnlyFunc(newSelect.getSelectItems());

		if(newSelect.getGroupBy() != null || hasFuncGroupBy)
		{
			newSelect = GroupByF(newSelect);
		}else if(newSelect.getDistinct() != null){
			newSelect = DistinctF(newSelect);
		}


		return newSelect;
	}

	/**
	 * A function which deals with the selection and projection queries
	 * 
	 * @param plainSelect the query as a PlainSelect object
	 * @return a PlainSelect object with the annotations
	 * @throws Exception
	 */
	private PlainSelect ProjectionSelectionF(PlainSelect plainSelect) throws Exception {
		SelectExpressionItem newColumn = new SelectExpressionItem();
		String prov = "";
		if(plainSelect.getFromItem() instanceof Table){
			Table tempTable = (Table) plainSelect.getFromItem();
			
			prov = "'"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " + (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName()) + ".prov";
		}else{
			SubSelect tempSubSelect = (SubSelect) plainSelect.getFromItem();
			tempSubSelect.setSelectBody(addAnnotations(tempSubSelect.getSelectBody()));

			prov = tempSubSelect.getAlias().getName()+".prov ";			
		}
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
	private PlainSelect JoinF(PlainSelect plainSelect, boolean correlated) throws Exception {
		String provToken = "";

        if(plainSelect.getFromItem() instanceof Table)
        {
            Table tempTable = (Table) plainSelect.getFromItem();

			if(!correlated)
            	provToken = "'" + tempTable.getFullyQualifiedName().replace('.',':') + ":' || " + (tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov";
        }else{
			SubSelect tempSubSelect = (SubSelect) plainSelect.getFromItem();
			tempSubSelect.setSelectBody(addAnnotations(tempSubSelect.getSelectBody()));
			provToken = tempSubSelect.getAlias() + ".prov";
		}

		for(Join join : plainSelect.getJoins()) {
			if(join.getRightItem() instanceof Table){            
                Table tempTable = (Table) join.getRightItem();
				if(join.isLeft()){
					String leftProv = "'" + tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov";
					
					provToken = "COALESCE(" + provToken + " || ' \u2297 ' || "+leftProv+", "+provToken+")";
				}else if(join.isRight()){
					String rightProv = "'"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov";
					provToken = "COALESCE(" + provToken + " || ' \u2297 ' || "+rightProv+", "+rightProv+")";
				}else{
                	provToken = provToken + " || ' . ' || '"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov";
				}
            }else{
				SubSelect tempSubSelect = (SubSelect) join.getRightItem();
				tempSubSelect.setSelectBody(addAnnotations(tempSubSelect.getSelectBody()));
			
				if(join.isLeft())
					provToken = "COALESCE(" + provToken + " || ' \u2297 ' || "+tempSubSelect.getAlias().getName()+".prov, "+provToken+")";
				else if(join.isRight())
					provToken = "COALESCE(" + provToken + " || ' \u2297 ' || "+tempSubSelect.getAlias().getName()+".prov, "+tempSubSelect.getAlias().getName()+".prov)";
				else if(correlated)
					provToken = tempSubSelect.getAlias().getName()+".prov";
				else
					provToken = provToken + "|| ' \u2297 ' || "+tempSubSelect.getAlias().getName()+".prov";

			}
		
		}

		SelectExpressionItem newColumn = new SelectExpressionItem();
		
		newColumn.setExpression(new net.sf.jsqlparser.schema.Column(provToken));
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

		List<SelectItem> selectItems = pHelper.getUnionColumns(setOperationList.getSelects().get(0));

		for(SelectBody select : setOperationList.getSelects()){
			addAnnotations(select);
		}

		for(SetOperation op : setOperationList.getOperations()){
			UnionOp union = (UnionOp) op;
			if(union.isAll() == false)
				union.setAll(true);
		}

		PlainSelect newSelect = new PlainSelect();
        SubSelect subSelect = new SubSelect();
         
		subSelect.setSelectBody(setOperationList);
        subSelect.setAlias(new Alias("_un"));
        newSelect.setFromItem(subSelect);
		GroupByElement groupByElements = new GroupByElement();

        for(SelectItem ex : selectItems){
			SelectExpressionItem expressionItem = (SelectExpressionItem) ex;
        	groupByElements.addGroupByExpressions(expressionItem.getExpression());
		}
		newSelect.setGroupByElement(groupByElements);

		Column prov = new Column();
		prov.setColumnName("prov");
		prov.setTable(new Table("_un"));
		
		SelectExpressionItem provItem  = new SelectExpressionItem(prov);
		provItem.setAlias(new Alias("prov"));
		selectItems.add(provItem);

		newSelect.setSelectItems(selectItems);
		return newSelect;		
	}	

	/**
	 * A function which deals with the queries with GROUP BY clause
	 * 
	 * @param newSelect the query as a PlainSelect object
	 * @return a PlainSelect object with the annotations
	 * @throws Exception
	 */
	private PlainSelect GroupByF(PlainSelect newSelect) throws Exception {
		ParserHelper pHelper = new ParserHelper();		

		FunctionProjection funcVisitor = new FunctionProjection();
		newSelect.getSelectItems().forEach(item -> item.accept(funcVisitor));
		String aggFunction = "";
		
		if(funcVisitor.hasFunction()) aggFunction = funcVisitor.getAggExpression();

		String firstColumn = pHelper.aggFunctionOrderBy(newSelect.getSelectItems().get(0));

		for (SelectItem selectItem : newSelect.getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {

				SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
				
				if(expressionItem.getAlias() != null && expressionItem.getAlias().getName() == "prov")
				{
					Column column = (Column) expressionItem.getExpression();
					expressionItem.setExpression(new Column(pHelper.getAggFunction(column.toString() + " " +aggFunction, '\u2295', firstColumn, dbname)));
				}
			}
		}

		if(funcVisitor.isMinMax())
		{
			newSelect = MinMaxF(newSelect, funcVisitor.getMinMaxColumn());
		}

		return newSelect;
	}

	/**
	 * A function which deals with the queries that contain in the Projection Min and Max functions and the apply their rules
	 * 
	 * @param newSelect the query as a PlainSelect object
	 * @param minMaxColumn the column inside the Min and Max functions
	 * @return a PlainSelect object with a new join in order to obtain the correct how-provenance result
	 * @throws AmbigousParserColumn
	 */
	private PlainSelect MinMaxF(PlainSelect newSelect, Column minMaxColumn) throws AmbigousParserColumn {
		if(minMaxColumn.getTable() == null) throw new AmbigousParserColumn();
		Column newColumn = new Column();
		PlainSelect copyNewSelect = new PlainSelect();

		for (SelectItem selectItem : newSelect.getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {
		
				SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
				
				if(expressionItem.getAlias() != null && expressionItem.getAlias().getName() == "prov")
				{
					continue;
				}else{
					if(expressionItem.getExpression() instanceof Function){
						if(expressionItem.getAlias() == null){
							
							SelectExpressionItem newExpressionItem = new SelectExpressionItem();
							newExpressionItem.setExpression(expressionItem.getExpression());
							newExpressionItem.setAlias(new Alias("MinMaxCol"));
							newColumn = new Column(newExpressionItem.getAlias().getName());
							copyNewSelect.addSelectItems(newExpressionItem);
						}else
							newColumn = new Column(expressionItem.getAlias().getName());
						
					}else
						copyNewSelect.addSelectItems(expressionItem);
					
				}
			}
		}

		copyNewSelect.setFromItem(newSelect.getFromItem());
		copyNewSelect.setGroupByElement(newSelect.getGroupBy());
		copyNewSelect.setHaving(newSelect.getHaving());
		copyNewSelect.setJoins(newSelect.getJoins());
		copyNewSelect.setWhere(newSelect.getWhere());
		
		SubSelect copySub = new SubSelect();
		copySub.setSelectBody(copyNewSelect);
		copySub.setAlias(new Alias("MinMax"));
		newColumn.setTable(new Table("MinMax"));

		Join newJoin = new Join();
		newJoin.setSimple(true);
		newJoin.setRightItem(copySub);
		newSelect.addJoins(newJoin);

		EqualsTo newCondition = new EqualsTo();
		newCondition.setLeftExpression(minMaxColumn);
		newCondition.setRightExpression(newColumn);

		Expression currentWhere = newSelect.getWhere();
		if (currentWhere == null) {
			newSelect.setWhere(newCondition);
		} else {
			newSelect.setWhere(new AndExpression(currentWhere, newCondition));
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

		String firstColumn = pHelper.aggFunctionOrderBy(newSelect.getSelectItems().get(0));
 		GroupByElement groupByElements = new GroupByElement();
		
		for (SelectItem selectItem : newSelect.getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {

				SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
				
				if(expressionItem.getAlias() != null && expressionItem.getAlias().getName() == "prov")
				{
					Column column = (Column) expressionItem.getExpression();
					expressionItem.setExpression(new Column(pHelper.getAggFunction(column.toString(), '\u2295', firstColumn, dbname)));
				}else if(expressionItem.getExpression() instanceof Column){
					groupByElements.addGroupByExpressions(expressionItem.getExpression());
				}
			}
		}
		newSelect.setGroupByElement(groupByElements);
		newSelect.setDistinct(null);
		return newSelect;
	}	

	private void CheckWhere(PlainSelect plainSelect) {
		ParserHelper ph = new ParserHelper();
		ExistsVisitor existsVisitor = new ExistsVisitor(ph.extractJoinTables(plainSelect));
		plainSelect.getWhere().accept(existsVisitor);

		

		if(existsVisitor.hasInClause()){
			if(existsVisitor.getInSubSelects().size() > 0){
				PlainSelect _plainSelect = new PlainSelect();
				List<Table> joinTables = ph.extractJoinTables(plainSelect);
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
					newJoin.setOuter(false);

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

		if(existsVisitor.hasExistsClause()){	
			
			if(existsVisitor.getParserExpressions().size() > 0){
				PlainSelect _plainSelect = new PlainSelect();
				List<Table> joinTables = ph.extractJoinTables(plainSelect);
				List<Table> toRemove = new ArrayList<>();
				List<Join> joins = new ArrayList<>();

				List<Expression> expressions = new ArrayList<>();

				for(ParserExpression pe : existsVisitor.getParserExpressions())
				{
					for(Table table : joinTables){
						if(ph.areTablesEqual(pe.getJoinTable(),table)){
							if(!(toRemove.size() > 0 && ph.areTablesEqual(toRemove.get(toRemove.size()-1),table))){
								if(_plainSelect.getFromItem() == null){
									_plainSelect.setFromItem(table);
								}else{
									Join _newJoin = new Join();
									_newJoin.setRightItem(table);
									_newJoin.setSimple(true);
									joins.add(_newJoin);
									_plainSelect.addJoins(joins);
								}

								toRemove.add(table);
							}			
							
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
							break;
						}
					}					
				}
				joinTables.removeAll(toRemove);
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

				if(plainSelect.getWhere() instanceof ExistsExpression)
				{
					plainSelect.setWhere(null);
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

		if(existsVisitor.hasSubSelect()){
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
				expressions.remove(0);
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
}