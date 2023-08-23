package com.generic.Parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.generic.Parser.ParserVisitors.ExistsVisitor;
import com.generic.Parser.ParserVisitors.FunctionProjection;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.UnionOp;
import net.sf.jsqlparser.util.TablesNamesFinder;

/** 
 * This class is responsible for parsing the SQL query
 * 
 * @author Paulo Pintor
 * @version 1.0
 * @since 1.0
*/
public class Parser {

	private List<List<ParserColumn>> projectionColumns;
	private int unionCounter = 0;

	/**
	 * The class constructor
	 * 
	 */
	public Parser(){
	}

	public String parseQuery(String query) throws Exception {
		Statement statement = CCJSqlParserUtil.parse(query);
        
		//List<String> tables = getTables(statement);

        String result = "";
		
        if (statement instanceof Select) {
			Select selectStatement = (Select) statement;
			projectionColumns = new ArrayList<List<ParserColumn>>();



			//[ ] It is not enough to be a SetOpeartionList, since Intersects are also a set of operations
			if(selectStatement.getSelectBody() instanceof SetOperationList)
            {
                SetOperationList setOperationList = (SetOperationList) selectStatement.getSelectBody();
            
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
					PlainSelect plainSelect = UnionProvenance(setOperationList);
					selectStatement.setSelectBody(plainSelect);
				}
            }else {                
                PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();

				if(plainSelect.getWhere() != null){
					CheckWhere(plainSelect);
				}

				//projectionColumns = initializeProjCols(getProjectionColumns(plainSelect));
                if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()){
                    plainSelect = JoinProvenance(plainSelect);
                    selectStatement.setSelectBody(plainSelect);
				}else{
					plainSelect = SelectProvenance(plainSelect);
					selectStatement.setSelectBody(plainSelect);
				}

            }

            result = selectStatement.getSelectBody().toString();
		}else{
			throw new InvalidParserOperation();
		}		

        //System.out.println(result);
        return result;
	}



	private void CheckWhere(PlainSelect plainSelect) {
		ExistsVisitor existsVisitor = new ExistsVisitor();
		plainSelect.getWhere().accept(existsVisitor);

		if(existsVisitor.hasInClause()){
			if(existsVisitor.getSubSelects().size() > 0){
				PlainSelect _plainSelect = new PlainSelect();
				List<Table> joinTables = extractJoinTables(plainSelect);
				List<Join> joins = new ArrayList<>();
				for(SubSelect select : existsVisitor.getSubSelects()){
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

	//Might be useful in the future
	private List<String> getTables(Statement stmnt) throws JSQLParserException {
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		return tablesNamesFinder.getTableList(stmnt);
	}

	//public PlainSelect JoinProvenance(PlainSelect plainSelect, String unionId) throws JSQLParserException, AmbigousParserColumn {
	public PlainSelect JoinProvenance(PlainSelect plainSelect) throws JSQLParserException, AmbigousParserColumn {
        String provToken = "";

        if(plainSelect.getFromItem() instanceof Table)
        {
            Table tempTable = (Table) plainSelect.getFromItem();
            //this.JoinWhereColumns(plainSelect, true);
			//this.SelectWhereColumns(plainSelect, null);
            provToken = "'" + tempTable.getFullyQualifiedName().replace('.',':') + ":' || " + (tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov";
        }else if(plainSelect.getFromItem() instanceof SubSelect){
            SubSelect tempSubSelect = (SubSelect) plainSelect.getFromItem();
            PlainSelect tempPlainSelect = (PlainSelect) (tempSubSelect).getSelectBody();
            
            //SubSelectWhereProvenance(tempPlainSelect, tempSubSelect.getAlias().getName());

            //TODO: falta verificar para UNIONS, DISTINCTS, etc
            if (tempPlainSelect.getJoins() != null && !tempPlainSelect.getJoins().isEmpty()){
				//this.SelectWhereColumns(tempPlainSelect, tempSubSelect.getAlias().getName(), unionId);
                //tempSubSelect.setSelectBody(JoinProvenance(tempPlainSelect, unionId));
				//this.SelectWhereColumns(tempPlainSelect, tempSubSelect.getAlias().getName());
                tempSubSelect.setSelectBody(JoinProvenance(tempPlainSelect));
			}else{
				//this.SelectWhereColumns(tempPlainSelect, tempSubSelect.getAlias().getName());
                tempSubSelect.setSelectBody(SelectProvenance(tempPlainSelect));
                //this.SelectWhereColumns(tempPlainSelect, tempSubSelect.getAlias().getName(), unionId);
                //tempSubSelect.setSelectBody(SelectProvenance(tempPlainSelect, unionId));
            }
            //[ ] confirmar: todos os subselects têm alias?
            provToken = tempSubSelect.getAlias().getName()+".prov";
        }

        List<Join> joins = plainSelect.getJoins();
       
        for(Join join : joins) {
            
            if(join.getRightItem() instanceof Table){
				//PlainSelect rightJoin = (PlainSelect) join.getRightItem();
				//this.SelectWhereColumns(rightJoin, "");
                //this.JoinWhereColumns(plainSelect, false);
            
                Table tempTable = (Table) join.getRightItem();
				if(join.isLeft()){
					provToken = "COALESCE(" + provToken + " || ' . ' || '"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov, "+provToken+")";
				}else if(join.isRight()){
					String leftProv = tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov";
					provToken = "COALESCE(" + provToken + " || ' . ' || "+leftProv+", "+leftProv+")";
				}else{
                	provToken = provToken + " || ' . ' || '"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " +(tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getFullyQualifiedName())+".prov";
				}
            }
            else if(join.getRightItem() instanceof SubSelect)
            {
				
                SubSelect tempSubSelect = (SubSelect) join.getRightItem();
                if(!tempSubSelect.getAlias().getName().toLowerCase().contains("minmax")){
					PlainSelect tempPlainSelect = (PlainSelect) (tempSubSelect).getSelectBody();
				
					//SubSelectWhereProvenance(tempPlainSelect, tempSubSelect.getAlias().getName());

					//TODO: falta verificar para UNIONS, DISTINCTS, etc
					if (tempPlainSelect.getJoins() != null && !tempPlainSelect.getJoins().isEmpty()){
						//this.SelectWhereColumns(tempPlainSelect, tempSubSelect.getAlias().getName());
						tempSubSelect.setSelectBody(JoinProvenance(tempPlainSelect));
					}else{
						//this.SelectWhereColumns(tempPlainSelect, tempSubSelect.getAlias().getName());
						tempSubSelect.setSelectBody(SelectProvenance(tempPlainSelect));
					}
					
					provToken = provToken + "|| ' . ' ||" + tempSubSelect.getAlias().getName()+".prov";
				}
            }
            //this.AddUnionColumns(plainSelect, false);
        }

        SelectExpressionItem newColumn = new SelectExpressionItem();
		
		FunctionProjection funcVisitor = new FunctionProjection();
		plainSelect.getSelectItems().forEach(item -> item.accept(funcVisitor));
		String aggFunction = "";
		
		if(funcVisitor.hasFunction()) aggFunction = funcVisitor.getAggExpression();

		String provenance ="'(' || "+provToken+" || ')' " + aggFunction;

        if(plainSelect.getGroupBy() != null || projectionOnlyFunc(plainSelect.getSelectItems())){
			SelectItem firstSelectItem = plainSelect.getSelectItems().get(0);
			String _column = "";
			
			if (firstSelectItem instanceof SelectExpressionItem) {
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) firstSelectItem;
				if(selectExpressionItem.getExpression() instanceof Column){
					Column column = (Column) selectExpressionItem.getExpression();
					_column = column.getFullyQualifiedName();			
				}else if(selectExpressionItem.getExpression() instanceof Function){
					_column = "1";
				}
			}
			String _listagg = getListAgg(provenance, '+', _column);
			newColumn.setExpression(new net.sf.jsqlparser.schema.Column(_listagg+ " as prov"));
        	plainSelect.addSelectItems(newColumn);
            return plainSelect;
        }else{
			newColumn.setExpression(new net.sf.jsqlparser.schema.Column(provenance + " as prov"));
        	plainSelect.addSelectItems(newColumn);
            return plainSelect;
		}
    }

	private PlainSelect UnionProvenance(SetOperationList setOperations) throws AmbigousParserColumn, JSQLParserException {
		List<SelectItem> selectItems = new ArrayList<>();
		List<Expression> groupByExpressions = new ArrayList<>();

		String unionID = "_un"+unionCounter;
		int index = 0;

		for(SelectBody select : setOperations.getSelects()){
			if(select instanceof PlainSelect){
				PlainSelect plainSelect = (PlainSelect) select;

				//ParserTable table = this.getTable(plainSelect);
				
				if(index == 0){
					for (SelectItem selectItem : plainSelect.getSelectItems()) {
						if (selectItem instanceof SelectExpressionItem) {
							SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
							Column column = (Column) selectExpressionItem.getExpression();
							
							if(index == 0){
								Column newColumn = new Column(column.getColumnName());
								newColumn.setTable(new Table(unionID));
								selectItems.add(new SelectExpressionItem(newColumn));
								groupByExpressions.add(newColumn);
							}
						}            
					}			

					Column firstColumn = (Column) ((SelectExpressionItem) plainSelect.getSelectItems().get(0)).getExpression();
					String listAgg = getListAgg("prov", '+', unionID+"."+firstColumn.getColumnName());
					selectItems.add(new SelectExpressionItem(new Column(listAgg)));
				}

				//UnionWhereColumns(plainSelect, index == 0 ? true : false,"", unionID);
				//UnionWhereColumns(plainSelect, index == 0 ? true : false,"");

				//plainSelect = SelectProvenance(plainSelect, unionID);
				plainSelect = SelectProvenance(plainSelect);
			}else if(select instanceof SubSelect){
				SubSelect tempSubSelect = (SubSelect) select;
				if(tempSubSelect.getSelectBody() instanceof PlainSelect){
					PlainSelect tempPlainSelect = (PlainSelect) (tempSubSelect).getSelectBody();	
					
					if(index == 0){
						for (SelectItem selectItem : tempPlainSelect.getSelectItems()) {
							if (selectItem instanceof SelectExpressionItem) {
								SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
								Column column = (Column) selectExpressionItem.getExpression();
								
								if(index == 0){
									Column newColumn = new Column(column.getColumnName());
									newColumn.setTable(new Table(unionID));
									selectItems.add(new SelectExpressionItem(newColumn));
									groupByExpressions.add(newColumn);
								}
							}            
						}
									

						Column firstColumn = (Column) ((SelectExpressionItem) tempPlainSelect.getSelectItems().get(0)).getExpression();
						String listAgg = getListAgg("prov", '+', unionID+"."+firstColumn.getColumnName());
						selectItems.add(new SelectExpressionItem(new Column(listAgg)));
					}
					
					//UnionWhereColumns(tempPlainSelect, index == 0 ? true : false, tempSubSelect.getAlias().getName());

					if (tempPlainSelect.getJoins() != null && !tempPlainSelect.getJoins().isEmpty())
					{
						tempPlainSelect = JoinProvenance(tempPlainSelect);
					}else{
						tempPlainSelect = SelectProvenance(tempPlainSelect);
					}
				}	
			}
			
			index++;
		}
		
		PlainSelect plainSelect = new PlainSelect();
        
        plainSelect.setSelectItems(selectItems);
        SubSelect subSelect = new SubSelect();
 
        //defining UNION ALL
		for(SetOperation op : setOperations.getOperations()){
			UnionOp union = (UnionOp) op;
			if(union.isAll() == false)
				union.setAll(true);
		}
        
		subSelect.setSelectBody(setOperations);
        subSelect.setAlias(new Alias(unionID));
        plainSelect.setFromItem(subSelect);
        for(Expression ex : groupByExpressions)
            plainSelect.addGroupByColumnReference(ex);

		unionCounter++;
        return plainSelect;
	}

    /**
     * The function receives as parameter a PlainSelect and adds a column called prov to statement.
     * 
     * @param plainSelect the select statement
     * @return the select statement with the prov column
     * @throws AmbigousParserColumn
     * @throws JSQLParserException
     * @throws Exception
     */
    //private PlainSelect SelectProvenance(PlainSelect plainSelect, String unionId) throws AmbigousParserColumn, JSQLParserException{
	private PlainSelect SelectProvenance(PlainSelect plainSelect) throws AmbigousParserColumn, JSQLParserException{
		if(plainSelect.getFromItem() instanceof Table){
			Table tempTable = (Table) plainSelect.getFromItem();
			if(plainSelect.getDistinct() != null){
				plainSelect = DistinctProvenance(plainSelect, tempTable.getAlias() != null ? tempTable.getAlias().getName() : tempTable.getFullyQualifiedName());
			}else if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty())
			{
                plainSelect = JoinProvenance(plainSelect);
				//plainSelect = JoinProvenance(plainSelect, unionId);
			}
			else if (projectionOnlyFunc(plainSelect.getSelectItems()))
			{
				FunctionProjection funcVisitor = new FunctionProjection();
				plainSelect.getSelectItems().forEach(item -> item.accept(funcVisitor));
				String aggFunction = "";
				
				if(funcVisitor.hasFunction()) aggFunction = funcVisitor.getAggExpression();

				String provenance = "'"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " + tempTable.getFullyQualifiedName()+".prov" + aggFunction;

				String _listagg = getListAgg(provenance, '+', "1");
				SelectExpressionItem newColumn = new SelectExpressionItem();
				newColumn.setExpression(new Column(_listagg+ " as prov"));
        		plainSelect.addSelectItems(newColumn);
			}else if(plainSelect.getGroupBy() != null)
			{
				//TODO este é o código do GROUP BY já se está a repetir duas vezes, colocar numa função
				SelectExpressionItem newColumn = new SelectExpressionItem();
				SelectItem firstSelectItem = plainSelect.getSelectItems().get(0);
				String _column = "";
				String provenance = "'"+tempTable.getFullyQualifiedName().replace('.',':') + ":' || " + tempTable.getFullyQualifiedName() + ".prov";
				if (firstSelectItem instanceof SelectExpressionItem) {
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) firstSelectItem;
					if(selectExpressionItem.getExpression() instanceof Column){
						Column column = (Column) selectExpressionItem.getExpression();
						_column = column.getFullyQualifiedName();			
					}else{ //(if(selectExpressionItem.getExpression() instanceof Function){
						FunctionProjection funcVisitor = new FunctionProjection();
						plainSelect.getSelectItems().forEach(item -> item.accept(funcVisitor));
						if(funcVisitor.hasFunction())
							provenance += funcVisitor.getAggExpression();
						_column = "1";
					}
				}
				String _listagg = getListAgg(provenance, '+', _column);
				newColumn.setExpression(new net.sf.jsqlparser.schema.Column("'(' ||"+_listagg+ "|| ')' as prov"));
				plainSelect.addSelectItems(newColumn);
			}else{
				SelectExpressionItem newColumn = new SelectExpressionItem();
				
				Column prov = new Column("prov");
				prov.setTable(tempTable);
				newColumn.setExpression(prov);
				plainSelect.addSelectItems(newColumn);
			}
		}else if(plainSelect.getFromItem() instanceof SubSelect){
			SubSelect tempSubSelect = (SubSelect) plainSelect.getFromItem();

			if(tempSubSelect.getSelectBody() instanceof SetOperationList)
            {
                SetOperationList setOperationList = (SetOperationList) tempSubSelect.getSelectBody();
            
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
					SubSelect newUnion = new SubSelect();
					newUnion.setSelectBody(UnionProvenance(setOperationList));
					newUnion.setAlias(tempSubSelect.getAlias());
					plainSelect.setFromItem(newUnion);
				}
			}else if(tempSubSelect.getSelectBody() instanceof PlainSelect){
				PlainSelect tempPlainSelect = (PlainSelect) (tempSubSelect).getSelectBody();
			
				if (tempPlainSelect.getJoins() != null && !tempPlainSelect.getJoins().isEmpty())
				{
					//SelectWhereColumns(tempPlainSelect, tempSubSelect.getAlias().getName(), unionId);
					//tempPlainSelect = JoinProvenance(tempPlainSelect, unionId);
					//SelectWhereColumns(tempPlainSelect, tempSubSelect.getAlias().getName());
					tempPlainSelect = JoinProvenance(tempPlainSelect);
				}else{
					//SelectWhereColumns(tempPlainSelect, tempSubSelect.getAlias().getName(), unionId);
					//tempPlainSelect = SelectProvenance(tempPlainSelect, unionId);
					//SelectWhereColumns(tempPlainSelect, tempSubSelect.getAlias().getName());
					tempPlainSelect = SelectProvenance(tempPlainSelect);
				}

			}			

			FunctionProjection funcVisitor = new FunctionProjection();
			plainSelect.getSelectItems().forEach(item -> item.accept(funcVisitor));
			String aggFunction = "";
				
			if(funcVisitor.hasFunction()) aggFunction = funcVisitor.getAggExpression();

			SelectExpressionItem newColumn = new SelectExpressionItem();

			if(plainSelect.getGroupBy() != null){
				SelectItem firstSelectItem = plainSelect.getSelectItems().get(0);
				String _column = "";
				
				if (firstSelectItem instanceof SelectExpressionItem) {
					SelectExpressionItem selectExpressionItem = (SelectExpressionItem) firstSelectItem;
					if(selectExpressionItem.getExpression() instanceof Column){
						Column column = (Column) selectExpressionItem.getExpression();
						_column = column.getFullyQualifiedName();			
					}else if(selectExpressionItem.getExpression() instanceof Function){
						_column = "1";
					}
				}
				String _listagg = getListAgg("'(' || "+tempSubSelect.getAlias().getName()+".prov "+aggFunction+" || ')'", '+', _column);
				newColumn.setExpression(new net.sf.jsqlparser.schema.Column(_listagg+ " as prov"));
				plainSelect.addSelectItems(newColumn);
				return plainSelect;
			}else{
				String prov = tempSubSelect.getAlias().getName()+".prov "+aggFunction;
				newColumn.setExpression(new Column(prov));
				plainSelect.addSelectItems(newColumn);
			}
		}

        return plainSelect;
    }
	
    private boolean projectionOnlyFunc(List<SelectItem> selectItems) {
		boolean result = false;
		if(selectItems.size() == 1){
			if(selectItems.get(0) instanceof SelectExpressionItem){
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItems.get(0);
				if(selectExpressionItem.getExpression() instanceof Function){
					result = true;
				}else if(selectExpressionItem.getExpression() instanceof Multiplication){
					result = true;
				}
			}
		}
		return result;
	}

	private PlainSelect DistinctProvenance(PlainSelect plainSelect, String alias) {
		List<Expression> groupByExpressions = new ArrayList<>();

		plainSelect.setDistinct(null);

		for (SelectItem selectItem : plainSelect.getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
				Column column = (Column) selectExpressionItem.getExpression();
				
				groupByExpressions.add(column);
			}            
		}
		Column firstColumn = (Column) ((SelectExpressionItem) plainSelect.getSelectItems().get(0)).getExpression();
		String listAgg = getListAgg("prov", '+', alias + "." + firstColumn.getColumnName());

		plainSelect.addSelectItems(new SelectExpressionItem(new Column("'(' || "+listAgg+" || ')' as prov")));

		for(Expression ex : groupByExpressions)
            plainSelect.addGroupByColumnReference(ex);

		return plainSelect;
	}

	

	/**
     * The function is responsible to deal with Group Bys. It receives as parameter a PlainSelect and creates a new select with the Group By columns and the prov column, surronding the select on the PlainSelect.
     * 
     * @param plainSelect the select statement
     * @return the select statement with the new Group By select and the prov column
     */
    private PlainSelect GroupByProvenance(PlainSelect groupBy){
		//FIXME - This function is not working properly, if it is just a simple select with a group by it surrounds the query and it is not needed.
		
        PlainSelect newPlainSelect = new PlainSelect();
        List<SelectItem> selectItems = new ArrayList<>();
        ExpressionList gpList = groupBy.getGroupBy().getGroupByExpressionList();
       
        int index = 0;
        Table tableTemp = null;
        String _column = new String();

        for(Expression ex : gpList.getExpressions()){
            if(ex instanceof Column){
                Column column = (Column) ex;
                
                if (index == 0){
                    tableTemp = new Table("_"+column.getTable().getName());
                    _column = tableTemp.getName()+"."+column.getColumnName();
                }
                column.setTable(tableTemp);
                selectItems.add(new SelectExpressionItem(column));                
            }
            index++;
        }
        
        String listAgg = getListAgg(tableTemp+".prov", '+', _column);
        selectItems.add(new SelectExpressionItem(new Column("'(' || "+listAgg+" || ')' as prov")));
        newPlainSelect.addSelectItems(selectItems);

        GroupByElement newGroupByElement = new GroupByElement();
        newGroupByElement.setGroupByExpressionList(gpList);
        newPlainSelect.setGroupByElement(newGroupByElement);

       
        SubSelect newSubSelect = new SubSelect();
        groupBy.setGroupByElement(null);
        newSubSelect.setSelectBody(groupBy);
        Alias alias = new Alias(tableTemp.getName());
        newSubSelect.setAlias(alias);
        newPlainSelect.setFromItem(newSubSelect);
        return newPlainSelect;
    }

    /**
     * The function receives a PlainSelect that contains a JOIN and stores the table(s) and its column(s). Calls the function WhereColumns to compare with the projection columns and set the columns and tables' names.
     * 
     * @param plainSelect the select statement
     * @param isFromItem a boolean to check if it is the fromItem or the rightItem following the parser logic
     * @throws AmbigousParserColumn
     */
	private void JoinWhereColumns(PlainSelect plainSelect, boolean isFromItem) throws AmbigousParserColumn{
        Table tableTemp = new Table();
        
        //TODO: the JOINS can be more than one?
        if(isFromItem) 
            tableTemp = (Table) plainSelect.getFromItem();
        else{
            List<Join> joins = plainSelect.getJoins();
            for(Join join : joins) {
                tableTemp = (Table) join.getRightItem();
            }
        }
            
        ParserTable table = new ParserTable(tableTemp.getName(), tableTemp.getAlias() != null ? tableTemp.getAlias().getName() : null);

        if(tableTemp.getSchemaName() != null) table.setSchema(tableTemp.getSchemaName());
        if(tableTemp.getDatabase() != null) table.setDatabase(tableTemp.getDatabase().getDatabaseName());        

        table.addColumns(plainSelect.getSelectItems());

        WhereColumns(table, tableTemp.getAlias() != null ? tableTemp.getAlias().getName() : "");
    }

    /**
     * The function receives a PlainSelect that contains a simple SELECT and stores the table(s) and its column(s). 
     * 
     * @param plainSelect the select statement
     * @param alias the alias of from a subselect
     * @throws AmbigousParserColumn
     */
    //private void SelectWhereColumns(PlainSelect plainSelect, String alias, String unionId) throws AmbigousParserColumn
	private void SelectWhereColumns(PlainSelect plainSelect, String alias) throws AmbigousParserColumn
	{
		List<ParserColumn> tempList = new ArrayList<>();
		//tempList = getProjectionColumns(plainSelect, unionId);
		tempList = getProjectionColumns(plainSelect);

		for(ParserColumn col : tempList){
			for(List<ParserColumn> projCol : projectionColumns){
				List<ParserColumn> filterTable = null;

				//if(unionId != null && !unionId.isEmpty())
				//	filterTable = projCol.stream().filter(column -> column.getTable().getName().equals(alias) && column.getUnionId().equals(unionId)).collect(Collectors.toList());
				//else
				filterTable = projCol.stream().filter(column -> column.getTable().getName().equals(alias)).collect(Collectors.toList());

				if(filterTable.size() > 0){
					for(ParserColumn column : filterTable){
						if(column.getName().equals(col.getAlias())){
							//int index = column.getOrder();
							//col.setOrder(index);
							projCol.set(projCol.indexOf(column), col);
						}else if(column.getName().equals(col.getName())){
							projCol.get(projCol.indexOf(column)).setTable(col.getTable());
						}
					}
				}
			}
		}

		removeRepeatedColumns();
    }

	//private void UnionWhereColumns(PlainSelect union, boolean first, String alias, String unionId) throws AmbigousParserColumn{
	private void UnionWhereColumns(PlainSelect union, boolean first, String alias) throws AmbigousParserColumn{
		List<ParserColumn> unionColumns = getProjectionColumns(union);

		if(projectionColumns.size() > 0)
		{
			if(alias.isEmpty() || alias == null){
				if(first){
					for(ParserColumn col : unionColumns){
						for(List<ParserColumn> projCol : projectionColumns){
							for(ParserColumn column : projCol){
								if(column.getName().equals(col.getAlias())){
									projCol.set(projCol.indexOf(column), col);
								}else if(column.getName().equals(col.getName())){
									projCol.get(projCol.indexOf(column)).setTable(col.getTable());
								}
							}
						}
					}
				}else{
					for(ParserColumn col : unionColumns){
						for(List<ParserColumn> projCol : projectionColumns){
							List<ParserColumn> filterTable = projCol.stream().filter(column -> column.getOrder() == col.getOrder()).collect(Collectors.toList());

							if(filterTable.size() > 0){
								projCol.add(col);
							}
						}
					}

				}
			}else{
				for(ParserColumn col : unionColumns){
					for(List<ParserColumn> projCol : projectionColumns){
						List<ParserColumn> filterTable = projCol.stream().filter(column -> column.getTable().getName().equals(alias)).collect(Collectors.toList());

						if(filterTable.size() > 0){
							for(ParserColumn column : filterTable){
								if(column.getName().equals(col.getAlias())){
									projCol.set(projCol.indexOf(column), col);
								}else if(column.getName().equals(col.getName())){
									projCol.get(projCol.indexOf(column)).setTable(col.getTable());
								}
							}
						}
					}
				}
			}
		}
		else
		{
			projectionColumns = initializeProjCols(unionColumns);
		}

		removeRepeatedColumns();
	}

	/*
				if(first){
				if(!alias.isEmpty()){
					for(List<ParserColumn> list : unionColumns){
						for(ParserColumn col : list){
							for(List<ParserColumn> projCol : projectionColumns){
						
								List<ParserColumn> filterTable = projCol.stream().filter(column -> column.getTable().getName().equals(alias)).collect(Collectors.toList());

								if(filterTable.size() > 0){
									for(ParserColumn column : filterTable){
										if(column.getName().equals(col.getAlias())){
											projCol.set(projCol.indexOf(column), col);
										}else if(column.getName().equals(col.getName())){
											int index = projCol.indexOf(column);
											projCol.get(index).setTable(col.getTable());
											projCol.get(index).setUnionId(col.getUnionId());
										}
									}
								}
							}
						}
					}
				}
			}else{
				if(!alias.isEmpty()){
					for(List<ParserColumn> list : unionColumns){
						for(ParserColumn col : list){
							for(List<ParserColumn> projCol : projectionColumns){
								List<ParserColumn> filterTable = projCol.stream().filter(column -> column.getTable().getName().equals(alias) && column.getUnionId().equals(unionId)).collect(Collectors.toList());

								if(filterTable.size() > 0){
									for(ParserColumn column : filterTable){
										if(column.getName().equals(col.getAlias())){
											projCol.set(projCol.indexOf(column), col);
										}else if(column.getName().equals(col.getName())){
											int index = projCol.indexOf(column);
											projCol.get(index).setTable(col.getTable());
											projCol.get(index).setUnionId(col.getUnionId());
										}
										else{
											projCol.add(col);
										}
									}
								}
							}
						}
					}					
				}else{
					for(List<ParserColumn> list : unionColumns){
						for(ParserColumn col : list){
							for(List<ParserColumn> projCol : projectionColumns){
								for(ParserColumn column : projCol){
									if(column.getName().equals(col.getAlias())){
										projCol.set(projCol.indexOf(column), col);
									}else if(column.getName().equals(col.getName())){
										int index = projCol.indexOf(column);
										projCol.get(index).setTable(col.getTable());
										projCol.get(index).setUnionId(col.getUnionId());
									}
									else{
										projCol.add(col);
									}
								}
							}
						}
					}
				}
			}
	private void UnionWhereColumns(List<List<ParserColumn>> unionColumns, String alias) throws AmbigousParserColumn{

		if(projectionColumns.size() > 0)
		{
			for(List<ParserColumn> list : unionColumns){
				for(ParserColumn col : list){
					for(List<ParserColumn> projCol : projectionColumns){
						if(!alias.isEmpty())
						{
							List<ParserColumn> filterTable = projCol.stream().filter(column -> column.getTable().getName().equals(alias)).collect(Collectors.toList());

							if(filterTable.size() > 0){
								for(ParserColumn column : filterTable){
									if(column.getName().equals(col.getAlias())){
										projCol.set(projCol.indexOf(column), col);
									}else if(column.getName().equals(col.getName())){
										projCol.get(projCol.indexOf(column)).setTable(col.getTable());
									}
								}
							}
						}else{

						}
					}
				}
			}
		}else{
			projectionColumns.addAll(unionColumns);
		}
	}*/


	private List<List<ParserColumn>> initializeProjCols(List<ParserColumn> unionColumns) {
		List<List<ParserColumn>> result = new ArrayList<>();
		for(ParserColumn col : unionColumns){
			List<ParserColumn> temp = new ArrayList<>();
			temp.add(col);
			result.add(temp);
		}
		return result;
	}

	private void removeRepeatedColumns() {
		for(int i = 0; i < projectionColumns.size(); i++){
			Set<ParserColumn> uniqueSet = new HashSet<>(projectionColumns.get(i));
			projectionColumns.set(i, new ArrayList<>(uniqueSet));
		}
	}

	/**
	 * The function receives a Plainselect and a String that represents the alias of the subselect. It compares the columns from the PlainSelect with the projection columns and set the columns and tables' names for the where-provenance. 
	 * 
	 * @param plainSelect the select statement
	 * @param alias the subselect alias
	 */
    private void SubSelectWhereProvenance(PlainSelect plainSelect, String alias){
        List<ParserColumn> tempColumns = null;//projectionColumns.stream().filter(column -> column.getTable().getName().compareTo(alias) == 0).collect(Collectors.toList());

        if(tempColumns.size() > 0){
            for(ParserColumn c : tempColumns){
                for(SelectItem selectItem : plainSelect.getSelectItems()){
                    if (selectItem instanceof SelectExpressionItem) {
				        SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                        Column column = (Column) selectExpressionItem.getExpression();
                        String columnName = column.getColumnName();
                        String columnAlias = selectExpressionItem.getAlias() != null ? selectExpressionItem.getAlias().getName() : null;

                        if(c.getName().equals(columnName) || c.getName().equals(columnAlias)){
                            ParserTable tableTemp = new ParserTable(column.getTable().getName(), column.getTable().getAlias() != null ? column.getTable().getAlias().getName() : null);

                            if(column.getTable().getSchemaName() != null) 
                                tableTemp.setSchema(column.getTable().getSchemaName());
                            if(column.getTable().getDatabase() != null)
                                tableTemp.setDatabase(column.getTable().getDatabase().getDatabaseName());
                            
                            c.setName(columnName);
                            c.setAlias(columnAlias);
                            c.setTable(tableTemp);
                        }
                    }
                }
            }
        }
    }

	private void UnionWhereProvenance(List<ParserColumn> unionColumns, String alias){
       /* if(!alias.isEmpty())
        {
            List<ParserColumn> tempColumns = null; //projectionColumns.stream().filter(column -> column.getTable().getName().compareTo(alias) == 0).collect(Collectors.toList());

            for(ParserColumn c : tempColumns){
                for(ParserColumn uc : unionColumns){
                    if(uc.getAlias() != null && c.getName().equals(uc.getAlias()) || c.getName().equals(uc.getName()))
                    {
                        for(ParserColumn _uc : unionColumns){
                            if(c.getOrder() == _uc.getOrder())
                            {
                                _uc.setOrder(c.getOrder());
                                //projectionColumns.add(_uc);
                            }
                        }
                        break;
                    }
                }
            }

            projectionColumns.removeAll(tempColumns);
        }else{
            for(ParserColumn c : projectionColumns){
                for(ParserColumn uc : unionColumns){
                    if(c.getName().equals(uc.getAlias()) || c.getName().equals(uc.getName()))
                    {
                        for(ParserColumn _uc : unionColumns){
                            if(c.getOrder() == _uc.getOrder() && !c.equals(_uc))
                            {
                                _uc.setOrder(c.getOrder());
                                projectionColumns.add(_uc);
                            }
                        }
                        break;
                    }
                }
            }
        } */
	}

	/**
	 * The function gets the table and the columns from a select (PlainSelect)
	 * 
	 * @param plainSelect the select statement
	 * @return the table with the columns
	 * @throws AmbigousParserColumn an excpetion if the column is ambigous
	 */
	/*private ParserTable getTable(PlainSelect plainSelect) throws AmbigousParserColumn{
        Table fromTable = (Table) plainSelect.getFromItem();

        ParserTable table = new ParserTable(fromTable.getName(), fromTable.getAlias() != null ? fromTable.getAlias().getName() : null);
        
        if(fromTable.getSchemaName() != null) table.setSchema(fromTable.getSchemaName());
        if(fromTable.getDatabase() != null) table.setDatabase(fromTable.getDatabase().getDatabaseName());        

        table.addColumns(plainSelect.getSelectItems());

        return table;
    }*/

    
    /**
     * The function receives a ParserTable and a String alias, and compares the columns of the table with the projectionColumns list and sets the actual name of the columns and table to use in the where provenance.
     * 
     * @param table the table
     * @param alias the alias of the table or the subselect
     */
    private void WhereColumns(ParserTable table, String alias){
        //if Alias is different of "", filter the projectionColumns
       /*if(!alias.isEmpty())
        {
            List<ParserColumn> tempColumns = projectionColumns.stream().filter(column -> column.getTable().getName().compareTo(alias) == 0).collect(Collectors.toList());
			
            for(ParserColumn c : tempColumns){
                for(ParserColumn uc : table.getColumns()){
                    System.out.println(c.getName() + " " + uc.getAlias() + " " + uc.getName());
                    if(c.getName().equals(uc.getAlias()) || c.getName().equals(uc.getName()))
                    {
                        c.setAlias(uc.getAlias());
                        c.setName(uc.getName());
                        c.setTable(table);
                    }
                }
            }
        }else{
            for(ParserColumn c : projectionColumns){
                for(ParserColumn uc : table.getColumns()){
                    if(c.getName().equals(uc.getAlias()) || c.getName().equals(uc.getName()))
                    {
                        c.setAlias(uc.getAlias());
                        c.setName(uc.getName());
                        c.setTable(table);
                    }
                }
            }
        }*/
    }

    /**
	 * The functions returns a list that contains list of columns by the order of the select statement
	 * 
	 * @param select the PlainSelect representing the select statement
	 * @param unionId if the select is a Union, it will need an Id to identify wich columns are part of wich Union
	 * 
	 * @return a list that contains list of columns by the order of the select statement
     * @throws AmbigousParserColumn
	 */
	//private List<List<ParserColumn>> getProjectionColumns(PlainSelect select, String unionId) throws AmbigousParserColumn{
	private List<ParserColumn> getProjectionColumns(PlainSelect select) throws AmbigousParserColumn{
		List<ParserColumn> result = new ArrayList<ParserColumn>();
	
		int index = 1;
		for (SelectItem selectItem : select.getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
				Column column = (Column) selectExpressionItem.getExpression();
				String columnName = column.getColumnName();
				String columnAlias = selectExpressionItem.getAlias() != null ? selectExpressionItem.getAlias().getName() : null;

				ParserColumn tempColumn = new ParserColumn(columnName, columnAlias, index);

				//if(unionId != null && !unionId.isEmpty())
				//	tempColumn.setUnionId(unionId);

				if(column.getTable() == null)
					throw new AmbigousParserColumn();
				else{
					ParserTable tableTemp = null;

					
					if(select.getFromItem() instanceof Table){
						Table fromTable = (Table) select.getFromItem();
						tableTemp = new ParserTable(fromTable.getName(), fromTable.getAlias() != null ? fromTable.getAlias().getName() : null);

						if(column.getTable().getSchemaName() != null) 
							tableTemp.setSchema(fromTable.getSchemaName());
						if(column.getTable().getDatabase() != null)
							tableTemp.setDatabase(fromTable.getDatabase().getDatabaseName());   
					}else{
						tableTemp = new ParserTable(column.getTable().getName());
						//TODO: Rever mas não parece necessário 
						/* 
						new ParserTable(column.getTable().getName(), column.getTable().getAlias() != null ? column.getTable().getAlias().getName() : null);

						if(column.getTable().getSchemaName() != null) 
							tableTemp.setSchema(column.getTable().getSchemaName());
						if(column.getTable().getDatabase() != null)
							tableTemp.setDatabase(column.getTable().getDatabase().getDatabaseName());  
						*/ 
					}
					tempColumn.setTable(tableTemp);
				}
                
				result.add(tempColumn);
                
				index++;
            }            
        }

		return result;
    }

    /**
     * The function generates a String with the SQL Standard function 'ListAGG'. The expression is the column to be aggregated, the separator is the character to separate the values and the orderByColumn is the column to order the values.
     * 
     * @param expression the colum to be aggregated
     * @param separator the character to separate the values
     * @param orderByColumn the column to order the values
     * @return a String with the SQL Standard function 'ListAGG'
     */
    private String getListAgg(String expression, char separator, String orderByColumn){
        //return String.format("listagg(%s, ' %c ') WITHIN GROUP (ORDER BY %s)", expression, separator, orderByColumn);
		return String.format("STRING_AGG(%s, ' %c ' ORDER BY %s)", expression, separator, orderByColumn);
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

	public void printWhere(){
		for(List<ParserColumn> l : projectionColumns){
			System.out.println(String.join(";", l.toString()));
		}
	}
}

/*
 *     private PlainSelect UnionProvenance(SetOperationList setOperations) throws Exception{
		List<SelectItem> selectItems = new ArrayList<>();
		List<Expression> groupByExpressions = new ArrayList<>();
		List<ParserColumn> unionColumns = new ArrayList<>();

		for(SelectBody select : setOperations.getSelects()){
			if(select instanceof PlainSelect){
				PlainSelect plainSelect = (PlainSelect) select;
				addProjectionColumns(plainSelect);
            	//Get the select table and the respectives columns
            	ParserTable table = this.getTable(plainSelect);

				//Add column PROV
				plainSelect = this.SelectProvenance(plainSelect);

				if(unionColumns.size() == 0){
					for(ParserColumn c : table.getColumns()){
						if(c.getAlias() != null){
							SelectExpressionItem itemTemp = new SelectExpressionItem(new Column(c.getAlias() != null ? c.getAlias() : c.getName()));
							selectItems.add(itemTemp);
						}else
							selectItems.add(new SelectExpressionItem(new Column(c.getName())));      

						groupByExpressions.add(new net.sf.jsqlparser.schema.Column(c.getAlias() != null ? c.getAlias() : c.getName()));
					}
					String listAgg = getListAgg("prov", '+', table.getColumns().get(0).getName());
					selectItems.add(new SelectExpressionItem(new Column(listAgg)));
				}

				for(ParserColumn c : table.getColumns()){
                    c.setTable(table);
                    unionColumns.add(c);
                }
			}
        }

		//UnionWhereProvenance(unionColumns, "");

		PlainSelect plainSelect = new PlainSelect();
        
        plainSelect.setSelectItems(selectItems);
        SubSelect subSelect = new SubSelect();
 
        //defining UNION ALL
        UnionOp union = (UnionOp) setOperations.getOperations().get(0);
        
		if(union.isAll() == false)
			union.setAll(true);
        
		subSelect.setSelectBody(setOperations);
        
        plainSelect.setFromItem(subSelect);
        for(Expression ex : groupByExpressions)
            plainSelect.addGroupByColumnReference(ex);

        return plainSelect;
	}

	if(!alias.isEmpty()){
					for(List<ParserColumn> list : unionColumns){
						for(ParserColumn col : list){
							for(List<ParserColumn> projCol : projectionColumns){
								List<ParserColumn> filterTable = projCol.stream().filter(column -> column.getTable().getName().equals(alias) && column.getUnionId().equals(unionId)).collect(Collectors.toList());

								if(filterTable.size() > 0){
									for(ParserColumn column : filterTable){
										if(column.getName().equals(col.getAlias())){
											projCol.set(projCol.indexOf(column), col);
										}else if(column.getName().equals(col.getName())){
											int index = projCol.indexOf(column);
											projCol.get(index).setTable(col.getTable());
											projCol.get(index).setUnionId(col.getUnionId());
										}
										else{
											projCol.add(col);
										}
									}
								}
							}
						}
					}					
				}else{
					for(List<ParserColumn> list : unionColumns){
						for(ParserColumn col : list){
							for(List<ParserColumn> projCol : projectionColumns){
								for(ParserColumn column : projCol){
									if(column.getName().equals(col.getAlias())){
										projCol.set(projCol.indexOf(column), col);
									}else if(column.getName().equals(col.getName())){
										int index = projCol.indexOf(column);
										projCol.get(index).setTable(col.getTable());
										projCol.get(index).setUnionId(col.getUnionId());
									}
									else{
										projCol.add(col);
									}
								}
							}
						}
					}
				}




				private static class ExistsVisitor extends ExpressionVisitorAdapter {
        private boolean hasExistsClause = false;
        private List<SubSelect> exists;

		public ExistsVisitor(){
			exists = new ArrayList<>();
		}

        @Override
        public void visit(ExistsExpression existsExpression) {
            hasExistsClause = true;

			SubSelect existExp = (SubSelect) existsExpression.getRightExpression();
            

			if(existExp.getSelectBody() instanceof PlainSelect){
				PlainSelect plainSelect = (PlainSelect) existExp.getSelectBody();

				
				FromItemVisitorAdapter fromVisitor = new FromItemVisitorAdapter() {
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

				existExp.getSelectBody().accept(new SelectVisitorAdapter(){
					@Override
					public void visit(PlainSelect plainSelect) {
						plainSelect.getFromItem().accept(fromVisitor);
						if (plainSelect.getJoins()!=null)
						   plainSelect.getJoins().forEach(join -> join.getRightItem().accept(fromVisitor));
					}
				});

				plainSelect.getWhere().accept(new ExpressionVisitorAdapter(){
					List<Expression> expressions = new ArrayList<>();
					@Override
					public void visit(EqualsTo equalsTo) {
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
						if(true)
						{
							expressions.add(equalsTo);
							equalsTo.setLeftExpression(new LongValue(1));
							equalsTo.setRightExpression(new LongValue(1));
						}
					}

					@Override
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
					}
				});
			}

			exists.add(existExp);
			
			//PlainSelect existSelelect = (PlainSelect) test.getSelectBody();

			/*existSelelect.accept(new SelectVisitorAdapter() {
                public void visit(SelectBody selectBody) {
                    selectBody.accept(new SelectItemVisitorAdapter() {
                        public void visit(Column column) {
                            String tableName = column.getTable().getName();
                            String attributeName = column.getColumnName();
                            involvedTables.add(tableName);
                            involvedAttributes.add(attributeName);
                        }
                    });
                }
			});
			
        }

        public boolean hasExistsClause() {
            return hasExistsClause;
        }

        public List<SubSelect> getExistsSelectBody() {
            return exists;
        }
    }
 */