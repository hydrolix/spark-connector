package io.hydrolix.spark.model;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;

public enum HdxValueType {
    // Note: These are mixed-case because `boolean` and `double` are reserved words.
    // the enum constant name must match the `hdxName` when lower-cased!
    Boolean("boolean"),
    Double("double"),
    Int8("int8"),
    Int32("int32"),
    Int64("int64"),
    String("string"),
    UInt8("uint8"),
    UInt32("uint32"),
    UInt64("uint64"),
    DateTime("datetime"),
    DateTime64("datetime64"),
    Epoch("epoch"),
    Array("array"),
    Map("map"),
    ;

    private static final java.util.Map<String, HdxValueType> byName = new HashMap<>();
    static {
        for (HdxValueType vt : HdxValueType.values()) {
            byName.put(vt.getHdxName(), vt);
        }
    }

    HdxValueType(String name) {
        this.hdxName = name;
    }

    private final String hdxName;

    @JsonValue
    public java.lang.String getHdxName() {
        return hdxName;
    }

    public static HdxValueType forName(String s) {
        if (!byName.containsKey(s)) throw new IllegalArgumentException("No enum value for " + s);
        return byName.get(s);
    }
}
