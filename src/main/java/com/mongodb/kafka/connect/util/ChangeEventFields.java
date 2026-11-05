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

import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.RawBsonDocument;
import org.bson.io.ByteBufferBsonInput;

import com.mongodb.lang.Nullable;

/**
 * Reads the fields the source task needs for every change event in one pass over the raw BSON.
 *
 * <p>{@link RawBsonDocument} field access ({@code containsKey}, {@code get}, {@code getDocument})
 * scans the document from the start on each call, and creating a {@code JsonWriter} per record is
 * expensive; at high event rates these dominate the task thread's CPU. The results are always
 * identical to the general {@link BsonDocument} accessors, which are used for anything other than
 * the normal shape of an event.
 */
public final class ChangeEventFields {
  private static final String ID_FIELD = "_id";
  private static final String RESUME_TOKEN_DATA_FIELD = "_data";
  private static final String NAMESPACE_FIELD = "ns";
  private static final String DOTTED_NAMESPACE_PREFIX = "ns.";
  private static final String DB_FIELD = "db";
  private static final String COLL_FIELD = "coll";

  /** A change event's {@code ns.db} and {@code ns.coll}; empty if absent or not strings. */
  public static final class Namespace {
    private final String db;
    private final String coll;

    private Namespace(final String db, final String coll) {
      this.db = db;
      this.coll = coll;
    }

    public String getDb() {
      return db;
    }

    public String getColl() {
      return coll;
    }
  }

  /**
   * @param changeStreamDocument the change event
   * @return exactly what {@code changeStreamDocument.getDocument("_id").toJson()} returns, and
   *     throws what it throws
   */
  public static String resumeTokenJson(final BsonDocument changeStreamDocument) {
    if (changeStreamDocument instanceof RawBsonDocument) {
      String json = hexResumeTokenJson((RawBsonDocument) changeStreamDocument);
      if (json != null) {
        return json;
      }
    }
    return changeStreamDocument.getDocument(ID_FIELD).toJson();
  }

  /** The JSON for the usual event shape: {@code _id} first, and exactly {_data: <hex string>}. */
  @Nullable
  private static String hexResumeTokenJson(final RawBsonDocument event) {
    try (BsonBinaryReader reader = reader(event)) {
      reader.readStartDocument();
      if (reader.readBsonType() != BsonType.DOCUMENT || !ID_FIELD.equals(reader.readName())) {
        return null;
      }
      reader.readStartDocument();
      if (reader.readBsonType() != BsonType.STRING
          || !RESUME_TOKEN_DATA_FIELD.equals(reader.readName())) {
        return null;
      }
      String data = reader.readString();
      if (reader.readBsonType() != BsonType.END_OF_DOCUMENT || !isHex(data)) {
        return null;
      }
      return "{\"_data\": \"" + data + "\"}";
    }
  }

  private static boolean isHex(final String value) {
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f'))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Reads {@code ns.db} and {@code ns.coll} in one pass, with the same result as looking up the
   * paths with {@link BsonDocumentFieldLookup#fieldLookup}.
   *
   * @return the namespace, or {@code null} if the event has a top-level field literally named
   *     {@code ns.<something>}, which takes precedence in {@code fieldLookup}: use that instead
   */
  @Nullable
  public static Namespace readNamespace(final RawBsonDocument event) {
    String db = "";
    String coll = "";
    boolean seenNamespace = false;
    try (BsonBinaryReader reader = reader(event)) {
      reader.readStartDocument();
      while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
        String name = reader.readName();
        if (name.startsWith(DOTTED_NAMESPACE_PREFIX)) {
          return null;
        }
        if (!seenNamespace
            && NAMESPACE_FIELD.equals(name)
            && reader.getCurrentBsonType() == BsonType.DOCUMENT) {
          seenNamespace = true;
          boolean seenDb = false;
          boolean seenColl = false;
          reader.readStartDocument();
          while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String field = reader.readName();
            boolean isDb = !seenDb && DB_FIELD.equals(field);
            boolean isColl = !seenColl && COLL_FIELD.equals(field);
            seenDb |= isDb;
            seenColl |= isColl;
            if ((isDb || isColl) && reader.getCurrentBsonType() == BsonType.STRING) {
              if (isDb) {
                db = reader.readString();
              } else {
                coll = reader.readString();
              }
            } else {
              reader.skipValue();
            }
          }
          reader.readEndDocument();
        } else {
          seenNamespace |= NAMESPACE_FIELD.equals(name);
          reader.skipValue();
        }
      }
    }
    return new Namespace(db, coll);
  }

  private static BsonBinaryReader reader(final RawBsonDocument document) {
    return new BsonBinaryReader(new ByteBufferBsonInput(document.getByteBuffer()));
  }

  private ChangeEventFields() {}
}
