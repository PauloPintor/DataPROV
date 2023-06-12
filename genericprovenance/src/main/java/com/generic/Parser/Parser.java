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
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
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

	private enum Operators {SELECT, UNION, DISTINCT, JOIN, GROUPBY}
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

			if(selectStatement.getSelectBody() instanceof SetOperationList)
            {
               /* SetOperationList setOperationList = (SetOperationList) select.getSelectBody();
                List<SetOperation> temp = setOperationList.getOperations();
                //It is not enough to be a SetOpeartionList, since Intersects are also a set of operations
                qs = new QueryParser(setOperationList);
                if(temp.get(0) instanceof UnionOp)
                    select.setSelectBody(qs.UnionProvenance(setOperationList, ""));*/ 
            }else {                
                PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        		projectionColumns = new ArrayList<ParserColumn>();
				addProjectionColumns(plainSelect);
                if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()){
                    plainSelect = JoinProvenance(plainSelect);
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
            
            subSelectWhereProvenance(tempPlainSelect, tempSubSelect.getAlias().getName());

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
            
                subSelectWhereProvenance(tempPlainSelect, tempSubSelect.getAlias().getName());

                //TODO: falta verificar para UNIONS, DISTINCTS, etc
                if (tempPlainSelect.getJoins() != null && !tempPlainSelect.getJoins().isEmpty()){
                    tempSubSelect.setSelectBody(JoinProvenance(tempPlainSelect));
                }else{
                    this.SelectWhereColumns(tempPlainSelect);
                    tempSubSelect.setSelectBody(SelectProvenance(tempPlainSelect));
                }
                
                provToken = provToken + "|| ' x ' ||" + tempSubSelect.getAlias().getName()+".prov";
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

    /**
     * The function receives as parameter a PlainSelect and adds a column called prov to statement.
     * 
     * @param plainSelect the select statement
     * @return the select statement with the prov column
     */
    private PlainSelect SelectProvenance(PlainSelect plainSelect) {
        SelectExpressionItem newColumn = new SelectExpressionItem();
        Table tempTable = (Table) plainSelect.getFromItem();
        String prov = (tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getName())+".prov";
        newColumn.setExpression(new Column(prov));
        plainSelect.addSelectItems(newColumn);

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


    private void subSelectWhereProvenance(PlainSelect plainSelect, String alias){
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
