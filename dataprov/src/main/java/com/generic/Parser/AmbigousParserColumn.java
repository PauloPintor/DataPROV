package com.generic.Parser;

/** 
 * Class responsible to throw an exception when projection columns are ambiguous
 * 
 * @author Paulo Pintor
 * @version 1.0
 * @since 1.0
*/
public class AmbigousParserColumn extends Exception{
	public AmbigousParserColumn() {
		super("All the columns on the projection need the table or subquery name - Ex: SELECT A.a FROM A - to avoid ambiguities");
	}
}
