/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.data.model;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class ExprBooleanValue implements ExprValue {
    private static final ExprValue TRUE = new ExprBooleanValue(true);
    private static final ExprValue FALSE = new ExprBooleanValue(false);

    private final Boolean value;

    private ExprBooleanValue(Boolean value) {
        this.value = value;
    }

    public static ExprValue ofTrue() {
        return TRUE;
    }

    public static ExprValue ofFalse() {
        return FALSE;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public ExprType type() {
        return ExprType.BOOLEAN;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}