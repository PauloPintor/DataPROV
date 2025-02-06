package com.Helper;

public class AggColumns {
	private String key;
    private String value;
	private String alias;

    public AggColumns(String key, String value, String alias) {
        this.key = key;
        this.value = value;
		this.alias = alias;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

	public String getAlias() {
        return alias;
    }
}
