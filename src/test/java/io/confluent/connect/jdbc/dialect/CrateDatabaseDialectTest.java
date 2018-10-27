/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Schema.Type;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

public class CrateDatabaseDialectTest extends BaseDialectTest<CrateDatabaseDialect> {

  @Override
  protected CrateDatabaseDialect createDialect() {
    return new CrateDatabaseDialect(sourceConfigWithUrl("jdbc:crate://something/"));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "BYTE");
    assertPrimitiveMapping(Type.INT16, "SHORT");
    assertPrimitiveMapping(Type.INT32, "INTEGER");
    assertPrimitiveMapping(Type.INT64, "LONG");
    assertPrimitiveMapping(Type.FLOAT32, "FLOAT");
    assertPrimitiveMapping(Type.FLOAT64, "DOUBLE");
    assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
    assertPrimitiveMapping(Type.BYTES, "BYTE");
    assertPrimitiveMapping(Type.STRING, "STRING");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "DOUBLE");
    assertDecimalMapping(3, "DOUBLE");
    assertDecimalMapping(4, "DOUBLE");
    assertDecimalMapping(5, "DOUBLE");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("BYTE", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SHORT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("LONG", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("DOUBLE", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("STRING", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BYTE", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DOUBLE", Decimal.schema(0));
    verifyDataTypeMapping("TIMESTAMP", Date.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("TIMESTAMP");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("TIMESTAMP");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("TIMESTAMP");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected =
        "CREATE TABLE \"myTable\" (\n" + "\"c1\" INTEGER NOT NULL,\n" + "\"c2\" LONG NOT NULL,\n" +
        "\"c3\" STRING NOT NULL,\n" + "\"c4\" STRING,\n" + "\"c5\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
        "\"c6\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
        "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" + "\"c8\" DOUBLE,\n" +
        "PRIMARY KEY(\"c1\"))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    System.out.print(sql);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {"ALTER TABLE \"myTable\" \n" + "ADD \"c1\" INTEGER NOT NULL,\n" +
                    "ADD \"c2\" LONG NOT NULL,\n" + "ADD \"c3\" STRING NOT NULL,\n" +
                    "ADD \"c4\" STRING,\n" + "ADD \"c5\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
                    "ADD \"c6\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
                    "ADD \"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
                    "ADD \"c8\" DOUBLE"};
    assertStatements(sql, statements);
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected1 = "INSERT INTO \"myTable\" (\"id1\",\"id2\",\"columnA\",\"columnB\"," +
                      "\"columnC\",\"columnD\") VALUES (?,?,?,?,?,?) ON CONFLICT (\"id1\"," +
                      "\"id2\") DO UPDATE SET \"columnA\"=\"columnA\",\"columnB\"=\"columnB\"," +
                      "\"columnC\"=\"columnC\",\"columnD\"=\"columnD\"";
    String sql1 = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected1, sql1);

    String expected2 = "INSERT INTO \"myTable\" (\"id1\",\"id2\",\"columnE\",\"columnF\"," +
            "\"columnG\") VALUES (?,?,?,?,?) ON CONFLICT (\"id1\"," +
            "\"id2\") DO UPDATE SET \"columnE\"=\"columnE\",\"columnF\"=\"columnF\"," +
            "\"columnG\"=\"columnG\"";
    String sql2 = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsEtoG);
    assertEquals(expected2, sql2);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" INTEGER NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," +
        System.lineSeparator() + "\"pk2\" INTEGER NOT NULL," + System.lineSeparator() +
        "\"col1\" INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(\"pk1\",\"pk2\"))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD \"newcol1\" INTEGER");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" " + System.lineSeparator() + "ADD \"newcol1\" INTEGER," +
        System.lineSeparator() + "ADD \"newcol2\" INTEGER DEFAULT 42");
  }

  @Test
  public void upsert() {
    TableId customer = tableId("Customer");
    assertEquals("INSERT INTO \"Customer\" (\"id\",\"name\",\"salary\",\"address\") " +
                 "VALUES (?,?,?,?) ON CONFLICT (\"id\") DO UPDATE SET \"name\"=\"name\"," +
                 "\"salary\"=\"salary\",\"address\"=\"address\"", dialect
                     .buildUpsertQueryStatement(customer, columns(customer, "id"),
                                                columns(customer, "name", "salary", "address")));
  }

  @Test
  public void bindFieldPrimitiveValues() throws SQLException {
    int index = ThreadLocalRandom.current().nextInt();
    verifyBindField(++index, Schema.INT8_SCHEMA, (byte) 42).setByte(index, (byte) 42);
    verifyBindField(++index, Schema.INT16_SCHEMA, (short) 42).setShort(index, (short) 42);
    verifyBindField(++index, Schema.INT32_SCHEMA, 42).setInt(index, 42);
    verifyBindField(++index, Schema.INT64_SCHEMA, 42L).setLong(index, 42L);
    verifyBindField(++index, Schema.BOOLEAN_SCHEMA, false).setBoolean(index, false);
    verifyBindField(++index, Schema.BOOLEAN_SCHEMA, true).setBoolean(index, true);
    verifyBindField(++index, Schema.FLOAT32_SCHEMA, -42f).setFloat(index, -42f);
    verifyBindField(++index, Schema.FLOAT64_SCHEMA, 42d).setDouble(index, 42d);
    verifyBindField(++index, Schema.BYTES_SCHEMA, new byte[]{42}).setBytes(index, new byte[]{42});
    verifyBindField(++index, Schema.BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{42})).setBytes(index, new byte[]{42});
    verifyBindField(++index, Schema.STRING_SCHEMA, "yep").setString(index, "yep");
    verifyBindField(++index, Decimal.schema(0), new BigDecimal("1.5").setScale(0, BigDecimal.ROUND_HALF_EVEN)).setBigDecimal(index, new BigDecimal(2));
    verifyBindField(++index, Date.SCHEMA, new java.util.Date(100)).setTimestamp(index, new java.sql.Timestamp(100), DateTimeUtils.UTC_CALENDAR.get());
    verifyBindField(++index, Time.SCHEMA, new java.util.Date(100)).setTimestamp(index, new java.sql.Timestamp(100), DateTimeUtils.UTC_CALENDAR.get());
    verifyBindField(++index, Timestamp.SCHEMA, new java.util.Date(100)).setTimestamp(index, new java.sql.Timestamp(100), DateTimeUtils.UTC_CALENDAR.get());
  }
}