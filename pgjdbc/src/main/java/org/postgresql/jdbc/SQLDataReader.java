/*
 * Copyright (c) 2024, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import static java.lang.Character.isWhitespace;

import org.postgresql.core.BaseConnection;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.SQLData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SQLDataReader {
  public @Nullable  <T> T read(@Nullable Object obj, Class<T> type, BaseConnection connection, TimestampUtils timestampUtils) throws SQLException {
    if (obj == null) {
      return null;
    }
    SQLData data;
    try {
      data = (SQLData) type.getConstructor().newInstance();
    } catch (Exception ex) {
      throw new SQLException(String.format("An accessible no-arg constructor is required for type [%s]", type), ex);
    }

    data.readSQL(new PgSQLInput(parse(obj.toString(), '(', ')'), connection, timestampUtils), data.getSQLTypeName());

    return type.cast(data);
  }

  public List<String> parseArray(String value) {
    return parse(value, '{', '}');
  }

  private List<String> parse(String value, char begin, char end) {
    List<String> values = new ArrayList<>();

    int len = value.length();
    StringBuilder builder = null;

    int lastDelimIdx = -1;

    scan: for (int charIdx = 0; charIdx < len; ++charIdx) {
      char ch = value.charAt(charIdx);
      if (ch == begin) {
        lastDelimIdx = charIdx;
      } else if (ch == end) {
        addTextElement(builder, lastDelimIdx, charIdx, values);
        break scan;
      } else if (ch == '"') {
        builder = new StringBuilder();
        charIdx = readString(value, charIdx, builder);
      } else if (ch == ',') {
        addTextElement(builder, lastDelimIdx, charIdx, values);
        builder = null;
        lastDelimIdx = charIdx;
      } else {
        //
        // ignore any whitespace we encounter
        //
        if (isWhitespace(ch)) {
          ++charIdx;
          while (charIdx < len && isWhitespace(value.charAt(charIdx))) {
            ++charIdx;
          }
          break;
        }

        if (builder == null) {
          builder = new StringBuilder();
        }
        builder.append(ch);
      }
    }
    return values;
  }

  private int readString(String value, int start, StringBuilder builder) {
    int len = value.length();
    int index;

    scan: for (index = start + 1; index < len; ++index) {
      char ch = value.charAt(index);
      switch (ch) {
        case '"':
          if (index < value.length() - 1 && value.charAt(index + 1) == '"') {
            ++index;
            builder.append('"');
            break;
          } else {
            break scan;
          }
        case '\\':
          ++index;
          if (index < value.length()) {
            ch = value.charAt(index);
          }
        default:
          builder.append(ch);
      }
    }

    return index;
  }

  private void addTextElement(@Nullable StringBuilder builder, int lastDelimIdx, int charIdx, List<String> values) {
    if (lastDelimIdx == charIdx - 1) {
      values.add(null);
    } else if (builder != null) {
      values.add(builder.toString());
    }
  }
}
