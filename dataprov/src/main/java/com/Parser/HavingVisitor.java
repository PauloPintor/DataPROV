package com.Parser;

import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;

public class HavingVisitor extends ExpressionVisitorAdapter<Object>{
	private boolean rewrite = false;
	private Object leftColumn;
	private Object rightColumn;
	private String operation;

	@Override
	public <S> Void visit(GreaterThan greaterThan, S context) {
		operation = "GreaterThan";
		leftColumn = greaterThan.getLeftExpression();
		if(greaterThan.getRightExpression() instanceof ParenthesedSelect) {
			rightColumn = greaterThan.getRightExpression();
			rewrite = true;
		}else {
			rightColumn = greaterThan.getRightExpression();
		}
		super.visit(greaterThan, context);	
		return null;
	}

	public boolean isRewrite() {
		return rewrite;
	}

	public Object getLeftColumn() {
		return leftColumn;
	}

	public Object getRightColumn() {
		return rightColumn;
	}

	public String getOperation() {
		return operation;
	}
}
