package com.Parser;

import java.util.List;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.UnionOp;

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
	private boolean isAll = false;
	/**
	 * The class constructor
	 * 
	 */
	public Parser(String dbname, boolean withTbInfo) {
		this.dbname = dbname;
		this.withTbInfo = withTbInfo;
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
	private PlainSelect addAnnotations(Object object, boolean correlated) throws Exception {
		PlainSelect newSelect = null;
		boolean isSubSelect = false;
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
			isAll = parenthesedSelect.getAlias() != null && parenthesedSelect.getAlias().getName().contains("AllT");
			newSelect = addAnnotations(parenthesedSelect.getSelect(), correlated);
			parenthesedSelect.setSelect(newSelect);
			isSubSelect = true;
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
		if(!isSubSelect)
		{
			ParserHelper pHelper = new ParserHelper();
			boolean hasFuncGroupBy = pHelper.projectionOnlyFunc(newSelect.getSelectItems());

			if(newSelect.getGroupBy() != null || hasFuncGroupBy)
			{
				newSelect = GroupByF(newSelect);
			}else if(newSelect.getDistinct() != null){
				newSelect = DistinctF(newSelect);
			}
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
		String prov = "";
		if(plainSelect.getFromItem() instanceof Table){
			Table tempTable = (Table) plainSelect.getFromItem();
			
			if(withTbInfo)
				prov = "'"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " + (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName()) + ".prov";
			else
				prov = (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName()) + ".prov";
		
		}else{
			ParenthesedSelect tempSubSelect = (ParenthesedSelect) plainSelect.getFromItem();
			tempSubSelect.setSelect(addAnnotations(tempSubSelect.getSelect(), false));
			
			prov = tempSubSelect.getAlias().getName()+".prov ";			
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
	private PlainSelect JoinF(PlainSelect plainSelect, boolean correlated) throws Exception {
		String provToken = "";

        if(plainSelect.getFromItem() instanceof Table)
        {
            Table tempTable = (Table) plainSelect.getFromItem();

			if(!correlated)
				if(withTbInfo)
            		provToken = "'" + tempTable.getFullyQualifiedName().replace('.',':') + ":' || " + (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
				else
					provToken = (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
				//provToken = (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
        }else{
			ParenthesedSelect tempSubSelect = (ParenthesedSelect) plainSelect.getFromItem();
			tempSubSelect.setSelect(addAnnotations(tempSubSelect.getSelect(), false));
			
			provToken = tempSubSelect.getAlias().getName()+".prov ";
		}
		boolean coalesce = false;

		for(Join join : plainSelect.getJoins()) {
			if(join.getRightItem() instanceof Table){            
                Table tempTable = (Table) join.getRightItem();
				if(join.isLeft()){
					String leftProv;
					if(withTbInfo) 
						leftProv = "'" + tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov";
					else
						leftProv = (tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov";

					provToken = "COALESCE('(' || " + provToken + " || ' "+(char) 0x2297+" ' || "+leftProv+" || ')', '(' ||"+provToken+"|| ')')";
					coalesce = true;
				}else if(join.isRight()){
					String rightProv;
					
					if(withTbInfo)
						rightProv = "'"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
					else
						rightProv = (tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";

					provToken = "COALESCE('(' || " + provToken + " || ' "+(char) 0x2297+" ' || "+rightProv+" || ')', '( ' || "+rightProv+") || ')')";
					coalesce = true;
				}else{
					if(withTbInfo)
                		provToken = provToken + " || ' "+(char) 0x2297+" ' || '"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
					else
						provToken = provToken + " || ' "+(char) 0x2297+" ' || " +(tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName())+".prov";
				}
            }else{
				ParenthesedSelect tempSubSelect = (ParenthesedSelect) join.getRightItem();
				if(tempSubSelect.getAlias() != null && tempSubSelect.getAlias().getName().contains("nestedT"))
					tempSubSelect.setSelect(addAnnotations(tempSubSelect, true));
				else
					tempSubSelect.setSelect(addAnnotations(tempSubSelect,false));
			
				if(join.isLeft()){
					provToken = "COALESCE('(' || " + provToken + " || ' "+(char) 0x2297+" ' || "+tempSubSelect.getAlias().getName()+".prov || ')', '(' || "+provToken+"|| ')')";
					
					coalesce = true;	
				}
				else if(join.isRight()){
					provToken = "COALESCE('(' || " + provToken + " || ' "+(char) 0x2297+" ' || "+tempSubSelect.getAlias().getName()+".prov|| ')', '(' || "+tempSubSelect.getAlias().getName()+".prov || ')')";
					
					coalesce = true;
				}
				else if(correlated){
					provToken = tempSubSelect.getAlias().getName()+".prov";
				}else{
					provToken = provToken + "|| ' "+(char) 0x2297+" ' || "+tempSubSelect.getAlias().getName()+".prov";
				}

			}
			
		}

		if(!coalesce && !correlated)
			provToken = "'(' || "+provToken+" || ')'";
		
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

		List<SelectItem<?>> selectItems = pHelper.getUnionColumns(setOperationList.getSelects().get(0));

		for(Select select : setOperationList.getSelects()){
			addAnnotations(select,false);
		}

		for(SetOperation op : setOperationList.getOperations()){
			UnionOp union = (UnionOp) op;
			if(union.isAll() == false)
				union.setAll(true);
		}

		PlainSelect newSelect = new PlainSelect();
		ParenthesedSelect parenthesedSelect = new ParenthesedSelect();
		parenthesedSelect.setSelect(setOperationList);
		parenthesedSelect.setAlias(new Alias("_un"));
		newSelect.setFromItem(parenthesedSelect);
		
		ExpressionList<Column> groupByList = new ExpressionList<Column>();
		for(SelectItem<?> ex : selectItems){
			groupByList.addExpression((Column) ex.getExpression());
		}
		
		GroupByElement groupBy = new GroupByElement();
		groupBy.setGroupByExpressions(groupByList);
		newSelect.setGroupByElement(groupBy);

		Column prov = new Column();
		prov.setColumnName("prov");
		prov.setTable(new Table("_un"));
		
		SelectItem<Column> provItem  = new SelectItem<Column>(prov);
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

		String aggFunction = "";
		FunctionProjection funcVisitor = new FunctionProjection();
		newSelect.getSelectItems().forEach(item -> item.accept(funcVisitor, null));
		if(funcVisitor.hasFunction() && !isAll) aggFunction = funcVisitor.getAggExpression();

		String firstColumn = pHelper.aggFunctionOrderBy(newSelect.getSelectItems().get(0));
		List<SelectItem<?>> newSelectItems = new ArrayList<>();
		for (SelectItem<?> selectItem : newSelect.getSelectItems()) {
			if (selectItem.getExpression() instanceof Column) {
				if(selectItem.getAlias() != null && selectItem.getAlias().getName() == "prov")
				{
					Column column = (Column) selectItem.getExpression();
					
					if(column.getTable() != null && column.getTable().getName().contains("_un")){
						SelectItem<Column> newColumn = new SelectItem<Column>(new Column(pHelper.getAggFunction(column.toString() + " " +aggFunction, (char) 0x2295, firstColumn, dbname)));
						newColumn.setAlias(new Alias("prov"));
						newSelectItems.add(newColumn);		
						
					}else{
						SelectItem<Column> newColumn = new SelectItem<Column>(new Column("'(' ||"+pHelper.getAggFunction(column.toString() + " " +aggFunction, (char) 0x2295, firstColumn, dbname)+"|| ')'"));
						newColumn.setAlias(new Alias("prov"));
						newSelectItems.add(newColumn);		
						
					}
				}else 
					newSelectItems.add(selectItem);
			}else
				newSelectItems.add(selectItem);
		}

		newSelect.setSelectItems(newSelectItems);
		
		if(funcVisitor.isMinMax() && !isAll)
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
		SelectItem<?> newColumn = new SelectItem<>();
		PlainSelect copyNewSelect = new PlainSelect();
		
		List<SelectItem<?>> restOfColumns = new ArrayList<>();
		
		for (SelectItem<?> selectItem : newSelect.getSelectItems()) {		
				if(selectItem.getAlias() != null && selectItem.getAlias().getName() == "prov")
				{
					continue;
				}else{
					if(selectItem.getExpression() instanceof Function){
						if(selectItem.getAlias() == null){
							newColumn = selectItem;
							newColumn.setAlias(new Alias("MinMaxCol"));
							copyNewSelect.addSelectItems(newColumn);
						}else{
							newColumn = selectItem;
							copyNewSelect.addSelectItems(newColumn);
						}	
						
					}else{
						copyNewSelect.addSelectItems(selectItem);
						restOfColumns.add(selectItem);
					}
					
			}
		}

		copyNewSelect.setFromItem(newSelect.getFromItem());
		copyNewSelect.setGroupByElement(newSelect.getGroupBy());
		copyNewSelect.setHaving(newSelect.getHaving());
		copyNewSelect.setJoins(newSelect.getJoins());
		copyNewSelect.setWhere(newSelect.getWhere());
		
		ParenthesedSelect copySub = new ParenthesedSelect();
		copySub.setSelect(copyNewSelect);
		copySub.setAlias(new Alias("MinMax"));

		Join newJoin = new Join();
		newJoin.setSimple(true);
		newJoin.setRightItem(copySub);
		
		List<Join> joins = new ArrayList<Join>();
		if (newSelect.getJoins() != null) joins.addAll(newSelect.getJoins());
		joins.add(newJoin);
		newSelect.setJoins(joins);
		
		EqualsTo newCondition = new EqualsTo();
		newCondition.setLeftExpression(minMaxColumn);

		Column joinColumn = new Column(newColumn.getAlias().getName());
		joinColumn.setTable(new Table("MinMax"));
		newCondition.setRightExpression(joinColumn);
		
		Expression currentWhere = newSelect.getWhere();
		if (currentWhere == null) {
			newSelect.setWhere(newCondition);
		} else {
			newSelect.setWhere(new AndExpression(currentWhere, newCondition));
		}

		for(SelectItem<?> item : restOfColumns){
			EqualsTo restCondition = new EqualsTo();
			restCondition.setLeftExpression(item.getExpression());
			Column restColumn = new Column(((Column)item.getExpression()).getColumnName());
			restColumn.setTable(new Table("MinMax"));
			restCondition.setRightExpression(restColumn);

			newSelect.setWhere(new AndExpression(newSelect.getWhere(), restCondition));
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
				SelectItem<Column> newColumn = new SelectItem<Column>(new Column("'(' ||"+pHelper.getAggFunction(column.toString(), (char) 0x2295, firstColumn, dbname)+"|| ')'"));
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
		return whereVisitor.identifyOperators(plainSelect);
	}
}


