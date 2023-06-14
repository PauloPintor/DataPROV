package com.generic.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SetOperationList.SetOperationType;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.UnionOp;

/** 
 * This class is responsible for parsing the SQL query
 * 
 * @author Paulo Pintor
 * @version 1.0
 * @since 1.0
*/
public class Parser {

	private List<ParserColumn> projectionColumns;

	/**
	 * The class constructor
	 * 
	 */
	public Parser(){
	}

	public String parseQuery(String query) throws Exception {
		Statement statement = CCJSqlParserUtil.parse(query);
        
        String result = "";
		
        if (statement instanceof Select) {
			Select selectStatement = (Select) statement;
			projectionColumns = new ArrayList<ParserColumn>();
            
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
				addProjectionColumns(plainSelect);
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

        System.out.println(result);
        return result;
	}

	public PlainSelect JoinProvenance(PlainSelect plainSelect) throws JSQLParserException, AmbigousParserColumn {
        String provToken = "";
        
        if(plainSelect.getFromItem() instanceof Table)
        {
            Table tempTable = (Table) plainSelect.getFromItem();
            this.JoinWhereColumns(plainSelect, true);
            provToken = (tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getName())+".prov";
        }else if(plainSelect.getFromItem() instanceof SubSelect){
            SubSelect tempSubSelect = (SubSelect) plainSelect.getFromItem();
            PlainSelect tempPlainSelect = (PlainSelect) (tempSubSelect).getSelectBody();
            
            SubSelectWhereProvenance(tempPlainSelect, tempSubSelect.getAlias().getName());

            //TODO: falta verificar para UNIONS, DISTINCTS, etc
            if (tempPlainSelect.getJoins() != null && !tempPlainSelect.getJoins().isEmpty()){
                tempSubSelect.setSelectBody(JoinProvenance(tempPlainSelect));
			}else{
                this.SelectWhereColumns(tempPlainSelect);
                tempSubSelect.setSelectBody(SelectProvenance(tempPlainSelect));
            }
            //[ ] confirmar: todos os subselects têm alias?
            provToken = tempSubSelect.getAlias().getName()+".prov";
        }

        List<Join> joins = plainSelect.getJoins();
       
        for(Join join : joins) {
            
            if(join.getRightItem() instanceof Table){
                this.JoinWhereColumns(plainSelect, false);
            
                Table tempTable = (Table) join.getRightItem();
                provToken = provToken + "|| ' x ' ||"+(tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getName())+".prov";
            }
            else if(join.getRightItem() instanceof SubSelect)
            {
                SubSelect tempSubSelect = (SubSelect) join.getRightItem();
                PlainSelect tempPlainSelect = (PlainSelect) (tempSubSelect).getSelectBody();
            
                SubSelectWhereProvenance(tempPlainSelect, tempSubSelect.getAlias().getName());

                //TODO: falta verificar para UNIONS, DISTINCTS, etc
                if (tempPlainSelect.getJoins() != null && !tempPlainSelect.getJoins().isEmpty()){
                    tempSubSelect.setSelectBody(JoinProvenance(tempPlainSelect));
                }else{
                    this.SelectWhereColumns(tempPlainSelect);
                    tempSubSelect.setSelectBody(SelectProvenance(tempPlainSelect));
                }
                
                provToken = provToken + "|| 'x' ||" + tempSubSelect.getAlias().getName()+".prov";
            }
            //this.AddUnionColumns(plainSelect, false);
        }

        SelectExpressionItem newColumn = new SelectExpressionItem();
        newColumn.setExpression(new net.sf.jsqlparser.schema.Column("'(' || "+provToken+" || ')' as prov"));
        plainSelect.addSelectItems(newColumn);

        if(plainSelect.getGroupBy() != null){
            return GroupByProvenance(plainSelect);
        }else
            return plainSelect;
    }

	private PlainSelect UnionProvenance(SetOperationList setOperations) throws AmbigousParserColumn, JSQLParserException {
		List<SelectItem> selectItems = new ArrayList<>();
		List<Expression> groupByExpressions = new ArrayList<>();
		//List<ParserColumn> unionColumns = new ArrayList<>();
		
		int index = 0;

		for(SelectBody select : setOperations.getSelects()){
			if(select instanceof PlainSelect){
				PlainSelect plainSelect = (PlainSelect) select;

				//ParserTable table = this.getTable(plainSelect);

				for (SelectItem selectItem : plainSelect.getSelectItems()) {
					if (selectItem instanceof SelectExpressionItem) {
						SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
						Column column = (Column) selectExpressionItem.getExpression();
						
						if(index == 0){
							Column newColumn = new Column(column.getColumnName());
							newColumn.setTable(new Table("_un"));
							selectItems.add(new SelectExpressionItem(newColumn));
							groupByExpressions.add(newColumn);
						}
					}            
				}

				plainSelect = SelectProvenance(plainSelect);
				
				if(index == 0){
					Column firstColumn = (Column) ((SelectExpressionItem) plainSelect.getSelectItems().get(0)).getExpression();
					String listAgg = getListAgg("prov", '+', "_un."+firstColumn.getColumnName());
					selectItems.add(new SelectExpressionItem(new Column(listAgg)));
				}
				index++;
			}

			
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
        subSelect.setAlias(new Alias("_un"));
        plainSelect.setFromItem(subSelect);
        for(Expression ex : groupByExpressions)
            plainSelect.addGroupByColumnReference(ex);

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
    private PlainSelect SelectProvenance(PlainSelect plainSelect) throws AmbigousParserColumn, JSQLParserException{
		if(plainSelect.getFromItem() instanceof Table){
			SelectExpressionItem newColumn = new SelectExpressionItem();
			Table tempTable = (Table) plainSelect.getFromItem();
			Column prov = new Column("prov");
			prov.setTable(tempTable);
			newColumn.setExpression(prov);
			plainSelect.addSelectItems(newColumn);
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
                    tempPlainSelect = JoinProvenance(tempPlainSelect);
				}
			}			

			SelectExpressionItem newColumn = new SelectExpressionItem();
			String prov = tempSubSelect.getAlias().getName()+".prov";
			newColumn.setExpression(new Column(prov));
			plainSelect.addSelectItems(newColumn);
		}

        return plainSelect;
    }
	
    /**
     * The function is responsible to deal with Group Bys. It receives as parameter a PlainSelect and creates a new select with the Group By columns and the prov column, surronding the select on the PlainSelect.
     * 
     * @param plainSelect the select statement
     * @return the select statement with the new Group By select and the prov column
     */
    private PlainSelect GroupByProvenance(PlainSelect groupBy){
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
     * The function receives a PlainSelect that contains a simple SELECT and stores the table(s) and its column(s). Calls the function WhereColumns to compare with the projection columns and set the columns and tables' names.
     * 
     * @param plainSelect the select statement
     * @param alias the alias of from a subselect
     * @throws AmbigousParserColumn
     */
    private void SelectWhereColumns(PlainSelect plainSelect) throws AmbigousParserColumn{
        Table tableTemp = (Table) plainSelect.getFromItem();

        ParserTable table = new ParserTable(tableTemp.getName(), tableTemp.getAlias() != null ? tableTemp.getAlias().getName() : null);

        if(tableTemp.getSchemaName() != null) table.setSchema(tableTemp.getSchemaName());
        if(tableTemp.getDatabase() != null) table.setDatabase(tableTemp.getDatabase().getDatabaseName());        

        table.addColumns(plainSelect.getSelectItems());

        WhereColumns(table, "");
    }


	/**
	 * The function receives a Plainselect and a String that represents the alias of the subselect. It compares the columns from the PlainSelect with the projection columns and set the columns and tables' names for the where-provenance. 
	 * 
	 * @param plainSelect the select statement
	 * @param alias the subselect alias
	 */
    private void SubSelectWhereProvenance(PlainSelect plainSelect, String alias){
        List<ParserColumn> tempColumns = projectionColumns.stream().filter(column -> column.getTable().getName().compareTo(alias) == 0).collect(Collectors.toList());

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
        if(!alias.isEmpty())
        {
            List<ParserColumn> tempColumns = projectionColumns.stream().filter(column -> column.getTable().getName().compareTo(alias) == 0).collect(Collectors.toList());

            for(ParserColumn c : tempColumns){
                for(ParserColumn uc : unionColumns){
                    if(uc.getAlias() != null && c.getName().equals(uc.getAlias()) || c.getName().equals(uc.getName()))
                    {
                        for(ParserColumn _uc : unionColumns){
                            if(c.getOrder() == _uc.getOrder())
                            {
                                _uc.setOrder(c.getOrder());
                                projectionColumns.add(_uc);
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
        }
	}

	/**
	 * The function gets the table and the columns from a select (PlainSelect)
	 * 
	 * @param plainSelect the select statement
	 * @return the table with the columns
	 * @throws AmbigousParserColumn an excpetion if the column is ambigous
	 */
	private ParserTable getTable(PlainSelect plainSelect) throws AmbigousParserColumn{
        Table fromTable = (Table) plainSelect.getFromItem();

        ParserTable table = new ParserTable(fromTable.getName(), fromTable.getAlias() != null ? fromTable.getAlias().getName() : null);
        
        if(fromTable.getSchemaName() != null) table.setSchema(fromTable.getSchemaName());
        if(fromTable.getDatabase() != null) table.setDatabase(fromTable.getDatabase().getDatabaseName());        

        table.addColumns(plainSelect.getSelectItems());

        return table;
    }

    
    /**
     * The function receives a ParserTable and a String alias, and compares the columns of the table with the projectionColumns list and sets the actual name of the columns and table to use in the where provenance.
     * 
     * @param table the table
     * @param alias the alias of the table or the subselect
     */
    private void WhereColumns(ParserTable table, String alias){
        //if Alias is different of "", filter the projectionColumns
        if(!alias.isEmpty())
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
        }
    }

    /**
	 * The functions adds the main projection columns to the list of projection columns
	 * 
	 * @param select the PlainSelect representing the select statement
	 */
	private void addProjectionColumns(PlainSelect select) throws Exception{
		int index = 0;
		for (SelectItem selectItem : select.getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
				Column column = (Column) selectExpressionItem.getExpression();
				String columnName = column.getColumnName();
				String columnAlias = selectExpressionItem.getAlias() != null ? selectExpressionItem.getAlias().getName() : null;

				ParserColumn tempColumn = new ParserColumn(columnName, columnAlias, index);

				if(column.getTable() == null)
					throw new AmbigousParserColumn();
				else{
					ParserTable tableTemp = new ParserTable(column.getTable().getName(), column.getTable().getAlias() != null ? column.getTable().getAlias().getName() : null);

					if(column.getTable().getSchemaName() != null) 
						tableTemp.setSchema(column.getTable().getSchemaName());
					if(column.getTable().getDatabase() != null)
						tableTemp.setDatabase(column.getTable().getDatabase().getDatabaseName());   

					tempColumn.setTable(tableTemp);
				}
                
				projectionColumns.add(tempColumn);
                
				index++;
            }            
        }
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
        return String.format("listagg(%s, '%c') WITHIN GROUP (ORDER BY %s)", expression, separator, orderByColumn);
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
 */