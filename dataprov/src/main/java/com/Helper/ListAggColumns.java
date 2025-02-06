package com.Helper;

import java.util.ArrayList;
import java.util.List;

public class ListAggColumns {
	private List<AggColumns> aggColumns;

	public ListAggColumns() {
		this.aggColumns = new ArrayList<>();
	}

	public void addAggColumns(String key, String value, String alias) {
		this.aggColumns.add(new AggColumns(key, value, alias));
	}

	public List<AggColumns> getAggColumns() {
		return this.aggColumns;
	}

	public boolean isEmpty() {
		return this.aggColumns.isEmpty();
	}

	public boolean containsAvg() {
		for (AggColumns aggColumn : this.aggColumns) {
			if (aggColumn.getValue().equals("avg")) {
				return true;
			}
		}
		return false;
	}

	public boolean containsSum() {
		for (AggColumns aggColumn : this.aggColumns) {
			if (aggColumn.getValue().equals("sum")) {
				return true;
			}
		}
		return false;
	}

	public boolean containsCount() {
		for (AggColumns aggColumn : this.aggColumns) {
			if (aggColumn.getValue().equals("count")) {
				return true;
			}
		}
		return false;
	}

	public boolean containsColumn(String column) {
		for (AggColumns aggColumn : this.aggColumns) {
			if (aggColumn.getKey().equals(column) || aggColumn.getAlias().equals(column)) {
				return true;
			}
		}
		return false;
	}
}