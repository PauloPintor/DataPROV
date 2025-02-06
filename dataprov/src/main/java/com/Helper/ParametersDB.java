package com.Helper;

public class ParametersDB {
    public enum Type { INT, TEXT, REAL, LONG }

    private Type type;

	private Object value;

	public ParametersDB(Type type, Object value) {
        this.type = type;
		this.value = value;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
}
