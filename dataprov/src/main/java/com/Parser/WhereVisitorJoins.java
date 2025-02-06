package com.Parser;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;

public class WhereVisitorJoins extends ExpressionVisitorAdapter<Expression>{
	private boolean nested = false;
	public WhereVisitorJoins(boolean nested) {
		this.nested = nested;
	}

	@Override
	public <S> Expression visit(ExpressionList<? extends Expression> expressionList, S context) {
		ExpressionList<Expression> newExpressionList = new ExpressionList<>();
		for (int i = 0; i < expressionList.size(); i++){
			Expression expression = expressionList.get(i).accept(this, context);
			if(expression != null) 
				if(expression instanceof AndExpression || expression instanceof OrExpression)
					newExpressionList.add(new ParenthesedExpressionList<>(expression));
				else
					newExpressionList.add(expression);
				
		}
		return  newExpressionList;
	}
	
	@Override
	public <S> Expression visit(AndExpression andExpression, S context) {
		Expression left = andExpression.getLeftExpression().accept(this, context);
		Expression right = andExpression.getRightExpression().accept(this, context);
		return right == null ? left : (left == null ? right : new AndExpression(left, right));
	}

	@Override
	public <S> Expression visit(OrExpression orExpression, S context) {
		Expression left = orExpression.getLeftExpression().accept(this, context);
		Expression right = orExpression.getRightExpression().accept(this, context);
		return right == null ? left : (left == null ? right : new OrExpression(left, right));
	}

	private boolean handleBinaryExpression(BinaryExpression	binaryExpression){
		if(binaryExpression.getLeftExpression() instanceof Column != false && binaryExpression.getRightExpression() instanceof Column != false){
			Column leftColumn = (Column) binaryExpression.getLeftExpression();
			Column rightColumn = (Column) binaryExpression.getRightExpression();
		
			String leftTable = leftColumn.getTable().getName();
			String rightTable = rightColumn.getTable().getName();
			if(!leftTable.equals(rightTable))
				return false;
		}else if(nested && (binaryExpression.getLeftExpression() instanceof ParenthesedSelect == true || binaryExpression.getRightExpression() instanceof ParenthesedSelect == true)){
			return false;
		}

		return true;
	}

	@Override
	public <S> Expression visit(InExpression inExpression, S context) {
		return inExpression.getLeftExpression() instanceof ParenthesedSelect || inExpression.getRightExpression() instanceof ParenthesedSelect ? null : inExpression;
	}

	@Override
	public <S> Expression visit(EqualsTo equalsTo, S context) {
		return handleBinaryExpression(equalsTo) ? equalsTo : null;
	}

	@Override
	public <S> Expression visit(GreaterThan greaterThan, S context) {
		return handleBinaryExpression(greaterThan) ? greaterThan : null;
	}

	@Override
	public <S> Expression visit(GreaterThanEquals greaterThanEquals, S context) {
		return handleBinaryExpression(greaterThanEquals) ? greaterThanEquals : null;
	}

	@Override
	public <S> Expression visit(MinorThan mintorThan, S context) {
		return handleBinaryExpression(mintorThan) ? mintorThan : null;
	}

	@Override
	public <S> Expression visit(MinorThanEquals mintorThanEquals, S context) {
		return handleBinaryExpression(mintorThanEquals) ? mintorThanEquals : null;
	}

	@Override
	public <S> Expression visit(NotEqualsTo notEqualsTo, S context) {
		return handleBinaryExpression(notEqualsTo) ? notEqualsTo : null;
	}

	@Override
	public <S> Expression visit(LikeExpression likeExpression, S context) {
		return handleBinaryExpression(likeExpression) ? likeExpression : null;
	}

	@Override
	public <S> Expression visit(Between between, S context) {
		return between;
	}
}
