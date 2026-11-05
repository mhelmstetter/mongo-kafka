/*
 * Copyright 2008-present MongoDB, Inc.
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
package com.mongodb.kafka.connect.util;

import static com.mongodb.kafka.connect.util.BsonDocumentFieldLookup.fieldLookup;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.bson.BsonDocument;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

class ChangeEventFieldsTest {
  private static final String REST =
      "\"operationType\": \"insert\", \"clusterTime\": {\"$timestamp\": {\"t\": 1, \"i\": 2}},"
          + " \"wallTime\": {\"$date\": 1000},"
          + " \"fullDocument\": {\"_id\": 1, \"ns\": {\"db\": \"inner\"}, \"a\": [1, {\"b\": 2}]},"
          + " \"ns\": {\"db\": \"mydb\", \"coll\": \"mycoll\"}, \"documentKey\": {\"_id\": 1}";

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"_id\": {\"_data\": \"8264ABCDEFabcdef0123456789\"}, " + REST + "}",
        "{\"_id\": {\"_data\": \"\"}, " + REST + "}",
        "{\"_id\": {\"_data\": \"not hex\"}, " + REST + "}",
        "{\"_id\": {\"_data\": \"quote\\\"back\\\\slash\\u00e9\"}, " + REST + "}",
        "{\"_id\": {\"_data\": \"82AB\", \"_typeBits\": {\"$binary\": "
            + "{\"base64\": \"QA==\", \"subType\": \"00\"}}}, "
            + REST
            + "}",
        "{\"_id\": {\"_data\": {\"$binary\": {\"base64\": \"QA==\", \"subType\": \"00\"}}}, "
            + REST
            + "}",
        "{\"_id\": {\"other\": \"82AB\"}, " + REST + "}",
        "{\"_id\": {}, " + REST + "}",
        "{\"operationType\": \"insert\", \"_id\": {\"_data\": \"82AB\"}}",
      })
  @DisplayName("resume token JSON is identical to BsonDocument.toJson")
  void resumeTokenJsonMatchesToJson(final String json) {
    RawBsonDocument raw = RawBsonDocument.parse(json);
    String expected = raw.getDocument("_id").toJson();
    assertEquals(expected, ChangeEventFields.resumeTokenJson(raw));
    assertEquals(expected, ChangeEventFields.resumeTokenJson(BsonDocument.parse(json)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"{\"a\": 1}", "{\"_id\": 1}", "{\"_id\": \"82AB\"}", "{}"})
  @DisplayName("a missing or non-document _id throws the same exception as getDocument")
  void resumeTokenJsonThrowsLikeGetDocument(final String json) {
    RawBsonDocument raw = RawBsonDocument.parse(json);
    BsonInvalidOperationException expected =
        assertThrows(BsonInvalidOperationException.class, () -> raw.getDocument("_id"));
    BsonInvalidOperationException actual =
        assertThrows(
            BsonInvalidOperationException.class, () -> ChangeEventFields.resumeTokenJson(raw));
    assertEquals(expected.getMessage(), actual.getMessage());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"_id\": {\"_data\": \"82AB\"}, " + REST + "}",
        "{\"ns\": {\"db\": \"mydb\"}}",
        "{\"ns\": {\"coll\": \"mycoll\"}}",
        "{\"ns\": {\"db\": 1, \"coll\": \"mycoll\"}}",
        "{\"ns\": {\"db\": \"mydb\", \"coll\": {\"x\": 1}}}",
        "{\"ns\": {}}",
        "{\"ns\": \"notadocument\"}",
        "{\"ns\": 1, \"a\": {\"ns\": {\"db\": \"nested\"}}}",
        "{\"a\": 1}",
        "{}",
      })
  @DisplayName("the one-pass namespace read matches fieldLookup")
  void readNamespaceMatchesFieldLookup(final String json) {
    RawBsonDocument raw = RawBsonDocument.parse(json);
    ChangeEventFields.Namespace namespace = ChangeEventFields.readNamespace(raw);
    assertEquals(stringAt("ns.db", raw), namespace.getDb());
    assertEquals(stringAt("ns.coll", raw), namespace.getColl());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"ns.db\": \"literal\", \"ns\": {\"db\": \"mydb\", \"coll\": \"mycoll\"}}",
        "{\"ns\": {\"db\": \"mydb\", \"coll\": \"mycoll\"}, \"ns.coll\": \"literal\"}",
      })
  @DisplayName("literal dotted top-level fields defer to fieldLookup, which prefers them")
  void literalDottedFieldsDeferToFieldLookup(final String json) {
    assertNull(ChangeEventFields.readNamespace(RawBsonDocument.parse(json)));
  }

  private static String stringAt(final String path, final BsonDocument document) {
    return fieldLookup(path, document)
        .filter(BsonValue::isString)
        .map(v -> v.asString().getValue())
        .orElse("");
  }
}
