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
			select.setSelectBody(addAnnotations(select.getSelectBody(), false));
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
	private PlainSelect addAnnotations(Object object, boolean correlated) throws Exception {
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
				plainSelect = CheckWhere(plainSelect);
			}
			 

			if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()){
				newSelect = JoinF(plainSelect, correlated);
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
			tempSubSelect.setSelectBody(addAnnotations(tempSubSelect.getSelectBody(),false));

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
			tempSubSelect.setSelectBody(addAnnotations(tempSubSelect.getSelectBody(),false));
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
				if(tempSubSelect.getAlias() != null && tempSubSelect.getAlias().getName().contains("nestedT"))
					tempSubSelect.setSelectBody(addAnnotations(tempSubSelect.getSelectBody(), true));
				else
					tempSubSelect.setSelectBody(addAnnotations(tempSubSelect.getSelectBody(),false));
			
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
			addAnnotations(select,false);
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

	private PlainSelect CheckWhere(PlainSelect plainSelect) {
		WhereVisitor whereVisitor = new WhereVisitor();
		return whereVisitor.identifyOperators(plainSelect);
	}
}