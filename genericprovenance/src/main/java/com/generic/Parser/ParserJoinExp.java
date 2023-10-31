package com.generic.Parser;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

public class ParserJoinExp {
	private Map<Table,List<Expression>> joinTblExp;

	public ParserJoinExp(){

	}

	public void setJoinTblExp(Table table,	Expression expression){
		if(this.joinTblExp == null){
			this.joinTblExp = new Hashtable<Table,List<Expression>>();
			List<Expression> expressions = new ArrayList<Expression>();
			expressions.add(expression);
			this.joinTblExp.put(table, expressions);
		}
		else
		{
			boolean _add = false;
			for(Table tbl : this.joinTblExp.keySet() ){
				if(areTablesEqual(tbl, table)){
					this.joinTblExp.get(tbl).add(expression);
					_add = true;
					break;
				}
			}

			if(!_add){
				List<Expression> expressions = new ArrayList<Expression>();
				expressions.add(expression);
				this.joinTblExp.put(table, expressions);
			}
				
		}
	}

	public void setJoinTblExp(Table table, List<Expression> expressions){
		if(this.joinTblExp == null){
			this.joinTblExp = new Hashtable<Table,List<Expression>>();
			this.joinTblExp.put(table, expressions);
		}
		else
		{
			if(this.joinTblExp.get(table) == null)
				this.joinTblExp.put(table, expressions);
			else
				this.joinTblExp.get(table).addAll(expressions);
		}
	}

	public static boolean areTablesEqual(Table table1, Table table2) {
        // Compare the table names
        String tableName1 = table1.getName();
        String tableName2 = table2.getName();

        // Compare the table aliases (if any)
        String alias1 = (table1.getAlias() != null) ? table1.getAlias().getName() : null;
        String alias2 = (table2.getAlias() != null) ? table2.getAlias().getName() : null;

        // Check if the names and aliases match
        return tableName1.equals(tableName2) && ((alias1 == null && alias2 == null) || alias1.equals(alias2));
    }

	public Map<Table,List<Expression>> getJoinTblExp(){
		return this.joinTblExp;
	}
}
