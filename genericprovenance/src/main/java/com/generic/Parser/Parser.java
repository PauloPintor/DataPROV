package com.generic.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
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
                    JoinProvenance(plainSelect);
				}

            }

            result = selectStatement.getSelectBody().toString();
		}else{
			throw new InvalidParserOperation();
		}		

        System.out.println(result);
        return result;
	}

	public PlainSelect JoinProvenance(PlainSelect plainSelect) throws JSQLParserException {
        String provToken = "";

        if(plainSelect.getFromItem() instanceof net.sf.jsqlparser.schema.Table)
        {
            Table tempTable = (Table) plainSelect.getFromItem();
            this.JoinWhereColumns(plainSelect, true);
            provToken = (tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getName())+".prov";
        }else if(plainSelect.getFromItem() instanceof SubSelect){
            SubSelect tempSubSelect = (SubSelect) plainSelect.getFromItem();
            PlainSelect tempPlainSelect = (PlainSelect) (tempSubSelect).getSelectBody();
            
            //TODO: falta verificar para UNIONS, DISTINCTS, etc
            if (tempPlainSelect.getJoins() != null && !tempPlainSelect.getJoins().isEmpty()){
                JoinProvenance(tempPlainSelect);
			}else{
                //tempSubSelect.setSelectBody(SelectProvenance(tempPlainSelect));
                //Table tempTable = (Table) tempPlainSelect.getFromItem();
                //provToken = (tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getName())+".prov";
            }
            //this.AddUnionColumns(plainSelect, true);
        }

        List<Join> joins = plainSelect.getJoins();
       
        for(Join join : joins) {
            if(join.getRightItem() instanceof net.sf.jsqlparser.schema.Table){
                this.JoinWhereColumns(plainSelect, false);
            
                Table tempTable = (Table) join.getRightItem();
                provToken = provToken + "|| ' x ' ||"+(tempTable.getAlias() != null ? tempTable.getAlias() : tempTable.getName())+".prov";
            }
            //else if(join.getRightItem() instanceof SubSelect)
                //this.AddUnionColumns(plainSelect, false);
        }

        SelectExpressionItem newColumn = new SelectExpressionItem();
        newColumn.setExpression(new net.sf.jsqlparser.schema.Column("'(' || "+provToken+" || ')' as prov"));
        plainSelect.addSelectItems(newColumn);

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
        newColumn.setExpression(new net.sf.jsqlparser.schema.Column("prov"));
        plainSelect.addSelectItems(newColumn);

        return plainSelect;
    }

    /**
     * The function receives a PlainSelect and stores the table(s) and its column(s). The table and columns are traversed and compared with the projection column list to get the actual name of the tables and columns to use in the where provenance.
     * 
     * @param plainSelect the select statement
     * @param isFromItem a boolean to check if it is the fromItem or the rightItem following the parser logic
     */
	private void JoinWhereColumns(PlainSelect plainSelect, boolean isFromItem){
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

        //if Alias is different of "", filter the projectionColumns
        if(tableTemp.getAlias() != null)
        {
            String alias = tableTemp.getAlias().getName();
            List<ParserColumn> tempColumns = projectionColumns.stream().filter(column -> column.getTable().getName().compareTo(alias) == 0).collect(Collectors.toList());
			
            for(ParserColumn c : tempColumns){
                for(ParserColumn uc : table.getColumns()){
                    if(c.getName() == uc.getAlias() || c.getName() == uc.getName())
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
                    if(c.getName() == uc.getAlias() || c.getName() == uc.getName())
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
}
