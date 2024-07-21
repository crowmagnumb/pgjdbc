/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.Oid;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public class PgStruct implements Struct {
  // if at least one of \ " ( ) (white space) , is present in the string, we need to quote the attribute value
  private static final Pattern NEEDS_ESCAPE_PATTERN = Pattern.compile("[\\\"()\\s,]");

  private final String sqlTypeName;
  private final PgStructDescriptor descriptor;
  private final Object[] attributes;

  private final TimestampUtils timestampUtils;
  private final Charset charset;

  public PgStruct(PgStructDescriptor descriptor, Object[] attributes, BaseConnection connection) {
    this.sqlTypeName = descriptor.sqlTypeName();
    this.descriptor = descriptor;
    this.attributes = attributes;
    this.timestampUtils = connection.getTimestampUtils();
    this.charset = Charset.forName(connection.getQueryExecutor().getEncoding().name());
    resolveAttributes();
  }

  private void resolveAttributes() {
    for (int i = 0; i < attributes.length; i++) {
      if (attributes[i] == null) {
        continue;
      }
      try {
        int oid = descriptor.pgAttributes()[i].oid();
        if (oid == Oid.DATE) {
          attributes[i] = timestampUtils.toDate(timestampUtils.getSharedCalendar(null), attributes[i].toString());
        } else if (oid == Oid.TIME) {
          attributes[i] = timestampUtils.toTime(timestampUtils.getSharedCalendar(null), attributes[i].toString());
        } else if (oid == Oid.TIMESTAMP || oid == Oid.TIMESTAMPTZ) {
          attributes[i] = timestampUtils.toTimestamp(timestampUtils.getSharedCalendar(null), attributes[i].toString());
        } else {
          attributes[i] = JavaObjectResolver.tryResolveObject(attributes[i], oid);
        }
      } catch (SQLException ignored) {
        // skip date time conversions that fail
      }
    }
  }

  @Override
  public String getSQLTypeName() {
    return sqlTypeName;
  }

  @Override
  public Object[] getAttributes() {
    return attributes;
  }

  public PgStructDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
    Object[] newAttributes = new Object[attributes.length];
    for (int i = 0; i < attributes.length; i++) {
      String type = descriptor.pgAttributes()[i].typeName();
      Class<?> javaType = map.get(type);
      if (javaType == null || attributes[i] == null) {
        // I guess if no type is found in the user mapping, we just return the attribute as is?
        newAttributes[i] = attributes[i];
        continue;
      }
      newAttributes[i] = getAttributeAs(attributes[i], javaType);
    }
    return newAttributes;
  }

  private Object getAttributeAs(Object attribute, Class<?> javaType) throws SQLException {
    if (javaType.isInstance(attribute)) {
      // no need to convert
      return attribute;
    }

    String value = String.valueOf(attribute);
    switch (javaType.getName()) {
      case "java.lang.Integer":
        return PgResultSet.toInt(value);
      case "java.lang.Long":
        return PgResultSet.toLong(value);
      case "java.lang.Double":
        return PgResultSet.toDouble(value);
      case "java.lang.BigDecimal":
        return PgResultSet.toBigDecimal(value);
      case "java.lang.String":
        return value;
      case "java.lang.Boolean":
        return BooleanTypeUtil.castToBoolean(value);
      case "java.util.Date":
        return timestampUtils.toTimestamp(null, value);
      default:
        throw new PSQLException(
            GT.tr("Unsupported conversion to {1}.", javaType.getName()),
            null);
    }
  }

  @Override
  public String toString() {
    PgAttribute[] pgAttributes = descriptor.pgAttributes();
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    for (int i = 0; i < pgAttributes.length; i++) {
      if (attributes[i] == null || attributes[i].toString() == null) {
        if (i < pgAttributes.length - 1) {
          sb.append(",");
        }
        continue;
      }

      int oid = pgAttributes[i].oid();

      String s;
      if (oid == Oid.BYTEA && attributes[i] instanceof byte[]) {
        s = new String((byte[]) attributes[i], charset);
      } else if (oid == Oid.BIT && attributes[i] instanceof Boolean) {
        s = ((Boolean) attributes[i]) ? "1" : "0";
      } else {
        s = attributes[i].toString();
      }

      boolean needsEscape = NEEDS_ESCAPE_PATTERN.matcher(s).find();
      if (needsEscape) {
        escapeStructAttribute(sb, s);
      } else {
        sb.append(s);
      }

      if (i < pgAttributes.length - 1) {
        sb.append(",");
      }
    }
    sb.append(")");
    return sb.toString();
  }

  public static void escapeStructAttribute(StringBuilder b, String s) {
    b.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '"' || c == '\\') {
        b.append(c);
      }

      b.append(c);
    }
    b.append('"');
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PgStruct pgStruct = (PgStruct) o;
    return Objects.equals(sqlTypeName, pgStruct.sqlTypeName) && Objects.deepEquals(attributes, pgStruct.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sqlTypeName, Arrays.hashCode(attributes));
  }
}
