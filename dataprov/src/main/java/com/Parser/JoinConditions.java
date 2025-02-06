package com.Parser;

import java.util.LinkedHashMap;
import java.util.Map;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;

public class JoinConditions {
	private LinkedHashMap<String, Expression> joins = new LinkedHashMap<>();
	private LinkedHashMap<String, LinkedHashMap<Expression, String>> temporary = new LinkedHashMap<>();
	private String mainTable;

	public JoinConditions(String mainTable) {
		this.mainTable = mainTable;
	}

	public void splitConditionsWithOperators(Expression expression) {
		splitConditionsWithOperators(expression, "");
		for (Map.Entry<String, LinkedHashMap<Expression, String>> entry : temporary.entrySet()) {
			String[] key = entry.getKey().split(",");
			int left = getIndexOfKey(key[0]);
			int right = getIndexOfKey(key[1]);
			if(left == -1 && right == -1)
			{
				joins.put(key[0], combineExpression(entry.getValue(),null));
			}else if(left != -1 && right == -1)
			{
				joins.put(key[1], combineExpression(entry.getValue(),null));
			}else if(left == -1 && right != -1)
			{
				joins.put(key[0], combineExpression(entry.getValue(),null));
			}else if(left > right)
			{
				joins.put(key[0], combineExpression(entry.getValue(),joins.get(key[0])));
			}else if(left < right)
			{
				joins.put(key[1], combineExpression(entry.getValue(),joins.get(key[1])));
			}
		}
	}

	private void splitConditionsWithOperators(Expression expression,String currentOperator)
	{
		if(expression instanceof ParenthesedExpressionList)
		{
			ExpressionList<Expression> expressionList = (ExpressionList<Expression>) expression;
			for (int i = 0; i < expressionList.size(); i++){
				splitConditionsWithOperators(expressionList.get(i), "");					
			}
		}
		else if (expression instanceof AndExpression) {
			AndExpression andExpression = (AndExpression) expression;
	
			// Recursively process left and right expressions with the "AND" operator
			splitConditionsWithOperators(andExpression.getLeftExpression(), "AND");
			splitConditionsWithOperators(andExpression.getRightExpression(), "AND");
	
		} else if (expression instanceof OrExpression) {
			OrExpression orExpression = (OrExpression) expression;
	
			// Recursively process left and right expressions with the "OR" operator
			splitConditionsWithOperators(orExpression.getLeftExpression(), "OR");
			splitConditionsWithOperators(orExpression.getRightExpression(), "OR");
	
		} else if (expression instanceof BinaryExpression) {
			BinaryExpression binaryExpression = (BinaryExpression) expression;

			if (binaryExpression.getLeftExpression() instanceof Column 
				&& binaryExpression.getRightExpression() instanceof Column) {
				Column leftColumn = (Column) binaryExpression.getLeftExpression();
				Column rightColumn = (Column) binaryExpression.getRightExpression();

				// Pass to joinCondition with the appropriate operator
				joinCondition(leftColumn, rightColumn, binaryExpression, currentOperator);
			}
		}
	}

	public LinkedHashMap<String, Expression> getJoinConditions() {
		return joins;
	}

	private void joinCondition(Column leftColumn, Column rightColumn, Expression exp, String currentOperator) {
		String leftTable = leftColumn.getTable().getName();
		String rightTable = rightColumn.getTable().getName();
	
		// Ensure we only process join conditions between different tables
		if (!leftTable.equals(rightTable)) {
			//String key = generateKey(leftTable, rightTable);
			if(leftTable.equals(mainTable))
			{
				if(joins.containsKey(rightTable)){
					Expression existingExp = joins.get(rightTable);
					if(!compareExpression(existingExp, exp)){
						Expression updatedExp = combineExpression(existingExp, exp, currentOperator);
						joins.put(rightTable, updatedExp);
					}					
				}else{
					joins.put(rightTable, exp);
				}
			}else if(rightTable.equals(mainTable))
			{
				if(joins.containsKey(leftTable)){
					Expression existingExp = joins.get(leftTable);
					if(!compareExpression(existingExp, exp)){
						Expression updatedExp = combineExpression(existingExp, exp, currentOperator);
						joins.put(leftTable, updatedExp);
					}
					//Expression updatedExp = combineExpression(existingExp, exp, currentOperator);
					//joins.put(leftTable, updatedExp);
				}else{
					joins.put(leftTable, exp);
				}
			}else{
				int left = getIndexOfKey(leftTable);
				int right = getIndexOfKey(rightTable);

				if(left == -1 && right == -1)
				{
					if(temporary.containsKey(generateKey(leftTable, rightTable)))
					{
						LinkedHashMap<Expression, String> temp = temporary.get(generateKey(leftTable, rightTable));
						temp.put(exp, currentOperator);
						temporary.put(generateKey(leftTable, rightTable), temp);
					}else if(temporary.containsKey(generateKey(rightTable, leftTable)))
					{
						LinkedHashMap<Expression, String> temp = temporary.get(generateKey(rightTable, leftTable));
						temp.put(exp, currentOperator);
						temporary.put(generateKey(rightTable, leftTable), temp);
					}else{
						LinkedHashMap<Expression, String> temp = new LinkedHashMap<>();
						temp.put(exp, currentOperator);
						temporary.put(generateKey(leftTable, rightTable), temp);
					}
				}else if(left != -1 && right == -1)
				{
					joins.put(rightTable, exp);
				}else if(left == -1 && right != -1)
				{
					joins.put(leftTable, exp);
				}else if(left > right)
				{
					Expression existingExp = joins.get(leftTable);
					if(!compareExpression(existingExp, exp)){
						Expression updatedExp = combineExpression(existingExp, exp, currentOperator);
						joins.put(leftTable, updatedExp);
					}
				}else if(left < right)
				{
					Expression existingExp = joins.get(rightTable);
					if(!compareExpression(existingExp, exp)){
						Expression updatedExp = combineExpression(existingExp, exp, currentOperator);
						joins.put(rightTable, updatedExp);
					}
				}
			}
		}
	}

	private boolean compareExpression(Expression existingExp, Expression newExp) {
		BinaryExpression existingBinaryExp = (BinaryExpression) existingExp;
		BinaryExpression newBinaryExp = (BinaryExpression) newExp;

		if(existingBinaryExp.toString().contains(newBinaryExp.toString()))
		{
			return true;
		}else{
			Expression left = newBinaryExp.getLeftExpression();
			Expression right = newBinaryExp.getRightExpression();
			newBinaryExp.setLeftExpression(newBinaryExp.getRightExpression());
			newBinaryExp.setRightExpression(left);
			if(existingBinaryExp.toString().contains(newBinaryExp.toString()))
			{
				return true;
			}else{
				newBinaryExp.setLeftExpression(left);
				newBinaryExp.setRightExpression(right);
			}
		}

		return false;
	}

	private int getIndexOfKey(String key) {
        int index = 0;
        for (String currentKey : joins.keySet()) {
            if (currentKey.equals(key)) {
                return index; // Found the key, return the index
            }
            index++;
        }
        return -1; // Key not found
    }
	
	private String generateKey(String table1, String table2) {
		// Ensure consistent key ordering for table pairs
		return table1.compareTo(table2) < 0 ? table1 + "," + table2 : table2 + "," + table1;
	}
	
	private Expression combineExpression(LinkedHashMap<Expression, String> expressions, Expression existingExp) {
		LinkedHashMap<Expression, String> temp = new LinkedHashMap<>();
		LinkedHashMap<Expression, String> orEntries = new LinkedHashMap<>();
		if(expressions.size() > 1){
			for (Map.Entry<Expression, String> entry : expressions.entrySet()) {
				if(entry.getValue().equals("OR"))
				{
					orEntries.put(entry.getKey(), entry.getValue());
				}else{
					temp.put(entry.getKey(), entry.getValue());
				}
			}

			temp.putAll(orEntries);
		}else{
			temp = expressions;
		}

		Expression combinedExp = null;
		int i = 0;
		for (Map.Entry<Expression, String> entry : temp.entrySet()) {
			if(i == 0 && existingExp == null){
				combinedExp = entry.getKey();
			}else if(i == 0 && existingExp != null){
				combinedExp = combineExpression(existingExp, entry.getKey(), entry.getValue());
			}else{
				combinedExp = combineExpression(combinedExp, entry.getKey(), entry.getValue());
			}
			i++;
		}

		return combinedExp;
	}

	private Expression combineExpression(Expression existingExp, Expression newExp, String operator) {
		// Combine expressions based on the operator
		switch (operator.toUpperCase()) {
			case "AND":
				return new AndExpression(existingExp, newExp);
			case "OR":
				return new OrExpression(existingExp, newExp);
			default:
				throw new IllegalArgumentException("Unsupported operator: " + operator);
		}
	}

}