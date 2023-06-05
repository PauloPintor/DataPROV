package com.generic.Parser;

/** 
 * Class responsible to throw an exception when the parser does not support the operation
 * 
 * @author Paulo Pintor
 * @version 1.0
 * @since 1.0
*/
public class InvalidParserOperation extends Exception{
	public InvalidParserOperation() {
        super("Invalid operation. The parser only supports SELECT, UNION, DISTINCT, JOIN and GROUP BY.");
    }
}
