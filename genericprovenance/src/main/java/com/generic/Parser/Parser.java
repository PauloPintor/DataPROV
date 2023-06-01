package com.generic.Parser;

import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

public class Parser {

	private enum Operators {SELECT, UNION, DISTINCT, JOIN, GROUPBY}
    private List<ParserColumn> projectionColumns;

	/**
	 * The class constructor
	 * 
	 * @param query The SQL query to parser
	 * @throws JSQLParserException
	 */
	public Parser(String query) throws JSQLParserException {
		Statement statement = CCJSqlParserUtil.parse(query);

        if (statement instanceof Select) {
			Select selectStatement = (Select) statement;
			// ...
		}
	}
}
