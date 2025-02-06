package com.dataprov;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Teste {
	public static void main( String[] args ) throws Exception
    {
		String query = "select supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment from part, supplier, partsupp, nation, region where part.p_partkey = partsupp.ps_partkey and supplier.s_suppkey = partsupp.ps_suppkey and part.p_size = 33 and nation.n_regionkey = region.r_regionkey or partsupp.ps_supplycost = (select min(partsupp.ps_supplycost) from partsupp, supplier, nation, region where part.p_partkey = partsupp.ps_partkey and supplier.s_suppkey = partsupp.ps_suppkey and supplier.s_nationkey = nation.n_nationkey and nation.n_regionkey = region.r_regionkey and region.r_name = 'ASIA') and region.r_name = 'ASIA' and part.p_type like '%BRASS' and supplier.s_nationkey = nation.n_nationkey order by supplier.s_acctbal desc, nation.n_name, supplier.s_name, part.p_partkey LIMIT 100;";

		// Parse the query
		Statement statement = CCJSqlParserUtil.parse(query);
		Select select = (Select) statement;
		PlainSelect plainSelect = (PlainSelect) select.getPlainSelect();
		Expression whereExpression = plainSelect.getWhere();

		// Split the conditions with operators
		splitConditionsWithOperators(whereExpression, "");
	}

	private static void splitConditionsWithOperators(Expression expression,String currentOperator)
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
	
		} else if (expression instanceof BinaryExpression) {
			BinaryExpression binaryExpression = (BinaryExpression) expression;
			System.out.println(binaryExpression.toString());
			System.out.println(currentOperator);
		}
	}
}
