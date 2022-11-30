/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;

import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


class ControllerResult<T> {
    private final List<ApiMessageAndVersion> records;
    private final T response;
    private final boolean isAtomic;
    private final boolean isTransaction;

    protected ControllerResult(List<ApiMessageAndVersion> records, T response, boolean isAtomic, boolean isTransaction) {
        Objects.requireNonNull(records);
        if (isTransaction && !isAtomic) {
            throw new IllegalArgumentException("Cannot create a transactional result that is not also atomic");
        }
        this.records = records;
        this.response = response;
        this.isAtomic = isAtomic;
        this.isTransaction = isTransaction;
    }

    public List<ApiMessageAndVersion> records() {
        return records;
    }

    public T response() {
        return response;
    }

    public boolean isAtomic() {
        return isAtomic;
    }

    public boolean isTransaction() {
        return isTransaction;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!o.getClass().equals(getClass()))) {
            return false;
        }
        ControllerResult other = (ControllerResult) o;
        return records.equals(other.records) &&
            Objects.equals(response, other.response) &&
            Objects.equals(isAtomic, other.isAtomic) &&
            Objects.equals(isTransaction, other.isTransaction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(records, response, isAtomic, isTransaction);
    }

    @Override
    public String toString() {
        return String.format(
            "ControllerResult(records=%s, response=%s, isAtomic=%s, isTransaction=%s)",
            records.stream().map(ApiMessageAndVersion::toString).collect(Collectors.joining(",")),
            response,
            isAtomic,
            isTransaction
        );
    }

    public ControllerResult<T> withoutRecords() {
        return new ControllerResult<>(Collections.emptyList(), response, false, false);
    }

    public static <T> ControllerResult<T> atomicOf(List<ApiMessageAndVersion> records, T response) {
        return new ControllerResult<>(records, response, true, false);
    }

    public static <T> ControllerResult<T> transactionOf(List<ApiMessageAndVersion> records, T response) {
        return new ControllerResult<>(records, response, true, true);
    }

    public static <T> ControllerResult<T> of(List<ApiMessageAndVersion> records, T response) {
        return new ControllerResult<>(records, response, false, false);
    }
}
