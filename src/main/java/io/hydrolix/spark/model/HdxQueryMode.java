/*
 * Copyright (c) 2023 Hydrolix Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hydrolix.spark.model;

public enum HdxQueryMode {
    /** Select row-oriented or columnar automatically based on the query schema */
    AUTO("auto"),
    /** Always use row-oriented mode */
    FORCE_ROW("force_row"),
    /** Always use columnar mode */
    FORCE_COLUMNAR("force_columnar"),
    ;

    private final String optionValue;

    HdxQueryMode(String name) {
        this.optionValue = name;
    }

    public String getOptionValue() {
        return optionValue;
    }

    public static HdxQueryMode of(String s) {
        switch(s.toLowerCase().trim()) {
            case "auto": return AUTO;
            case "force_row": return FORCE_ROW;
            case "force_columnar": return FORCE_COLUMNAR;
            default: throw new IllegalArgumentException("No HdxQueryMode value matches `" + s + "`");
        }
    }
}
