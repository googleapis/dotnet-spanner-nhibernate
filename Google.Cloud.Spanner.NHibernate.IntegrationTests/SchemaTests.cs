// Copyright 2021 Google LLC
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     https://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.Spanner.Connection;
using System.Data;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests
{
    public class SchemaTests : IClassFixture<SpannerSampleFixture>
    {
        private readonly SpannerSampleFixture _fixture;

        public SchemaTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [Fact]
        public void CanDropAndRecreateSchema()
        {
            var initialSchema = GetCurrentSchema();
            
            var exporter = new SpannerSchemaExport(_fixture.Configuration);
            exporter.Execute(false, true, false);
            
            VerifySchemaEquality(initialSchema);
        }

        [Fact]
        public async Task CanDropAndRecreateSchemaAsync()
        {
            var initialSchema = GetCurrentSchema();
            
            var exporter = new SpannerSchemaExport(_fixture.Configuration);
            await exporter.ExecuteAsync(false, true, false);
            
            VerifySchemaEquality(initialSchema);
        }

        [Fact]
        public void CanAddMissingTable()
        {
            var initialSchema = GetCurrentSchema();
            
            // Drop a table and then execute a SchemaUpdate to recreate it.
            using var connection = new SpannerRetriableConnection(_fixture.GetConnection());
            var cmd = connection.CreateDdlCommand("DROP TABLE Performances");
            cmd.ExecuteNonQuery();
            var updater = new SpannerSchemaUpdate(_fixture.Configuration);
            updater.Execute(false, true);
            
            Assert.Empty(updater.Exceptions);
            VerifySchemaEquality(initialSchema);
        }

        [Fact]
        public async Task CanAddMissingTableAsync()
        {
            var initialSchema = GetCurrentSchema();
            
            // Drop a table and then execute a SchemaUpdate to recreate it.
            using var connection = new SpannerRetriableConnection(_fixture.GetConnection());
            var cmd = connection.CreateDdlCommand("DROP TABLE Performances");
            await cmd.ExecuteNonQueryAsync();
            var updater = new SpannerSchemaUpdate(_fixture.Configuration);
            await updater.ExecuteAsync(false, true);
            
            Assert.Empty(updater.Exceptions);
            VerifySchemaEquality(initialSchema);
        }

        [Fact]
        public void CanAddMissingColumn()
        {
            var initialSchema = GetCurrentSchema();
            // Drop a table and then execute a SchemaUpdate to recreate it.
            using var connection = new SpannerRetriableConnection(_fixture.GetConnection());
            var cmd = connection.CreateDdlCommand("DROP TABLE Performances");
            cmd.ExecuteNonQuery();
            var updater = new SpannerSchemaUpdate(_fixture.Configuration);
            updater.Execute(false, true);
            
            Assert.Empty(updater.Exceptions);
            VerifySchemaEquality(initialSchema);
        }

        struct Schema
        {
            public DataTable Tables;
            public DataTable Columns;
            public DataTable ColumnOptions;
            public DataTable Indexes;
            public DataTable IndexColumns;
            public DataTable ReferentialConstraints;
        }

        private Schema GetCurrentSchema()
        {
            using var connection = new SpannerRetriableConnection(_fixture.GetConnection());
            var schema = new Schema
            {
                Tables = connection.GetSchema("Tables"),
                Columns = connection.GetSchema("Columns"),
                ColumnOptions = connection.GetSchema("ColumnOptions"),
                Indexes = connection.GetSchema("Indexes"),
                IndexColumns = connection.GetSchema("IndexColumns"),
                ReferentialConstraints = connection.GetSchema("ReferentialConstraints"),
            };

            return schema;
        }

        private void VerifySchemaEquality(Schema initialSchema)
        {
            using var connection = new SpannerRetriableConnection(_fixture.GetConnection());
            var schema = GetCurrentSchema();
            
            Assert.Equal(initialSchema.Tables.Rows.Count, schema.Tables.Rows.Count);
            for (var row = 0; row < initialSchema.Tables.Rows.Count; row++)
            {
                Assert.Equal(initialSchema.Tables.Rows[row]["TABLE_NAME"], schema.Tables.Rows[row]["TABLE_NAME"]);
            }
            
            Assert.Equal(initialSchema.Columns.Rows.Count, schema.Columns.Rows.Count);
            for (var i = 0; i < initialSchema.Columns.Rows.Count; i++)
            {
                Assert.Equal(initialSchema.Columns.Rows[i]["TABLE_NAME"], schema.Columns.Rows[i]["TABLE_NAME"]);
                Assert.Equal($"{initialSchema.Columns.Rows[i]["TABLE_NAME"]}.{initialSchema.Columns.Rows[i]["COLUMN_NAME"]}",
                    $"{schema.Columns.Rows[i]["TABLE_NAME"]}.{schema.Columns.Rows[i]["COLUMN_NAME"]}");
                Assert.Equal(initialSchema.Columns.Rows[i]["COLUMN_NAME"], schema.Columns.Rows[i]["COLUMN_NAME"]);
                Assert.Equal(initialSchema.Columns.Rows[i]["ORDINAL_POSITION"], schema.Columns.Rows[i]["ORDINAL_POSITION"]);
                Assert.Equal(initialSchema.Columns.Rows[i]["IS_NULLABLE"], schema.Columns.Rows[i]["IS_NULLABLE"]);
                Assert.Equal(initialSchema.Columns.Rows[i]["SPANNER_TYPE"], schema.Columns.Rows[i]["SPANNER_TYPE"]);
                Assert.Equal(initialSchema.Columns.Rows[i]["COLUMN_DEFAULT"], schema.Columns.Rows[i]["COLUMN_DEFAULT"]);
                Assert.Equal(initialSchema.Columns.Rows[i]["IS_GENERATED"], schema.Columns.Rows[i]["IS_GENERATED"]);
                Assert.Equal(initialSchema.Columns.Rows[i]["GENERATION_EXPRESSION"], schema.Columns.Rows[i]["GENERATION_EXPRESSION"]);
            }
            
            Assert.Equal(initialSchema.ColumnOptions.Rows.Count, schema.ColumnOptions.Rows.Count);
            for (var i = 0; i < initialSchema.ColumnOptions.Rows.Count; i++)
            {
                Assert.Equal(initialSchema.ColumnOptions.Rows[i][0], schema.ColumnOptions.Rows[i][0]);
                Assert.Equal(initialSchema.ColumnOptions.Rows[i]["TABLE_NAME"], schema.ColumnOptions.Rows[i]["TABLE_NAME"]);
                Assert.Equal(initialSchema.ColumnOptions.Rows[i]["COLUMN_NAME"], schema.ColumnOptions.Rows[i]["COLUMN_NAME"]);
                Assert.Equal(initialSchema.ColumnOptions.Rows[i]["OPTION_NAME"], schema.ColumnOptions.Rows[i]["OPTION_NAME"]);
                Assert.Equal(initialSchema.ColumnOptions.Rows[i]["OPTION_TYPE"], schema.ColumnOptions.Rows[i]["OPTION_TYPE"]);
                Assert.Equal(initialSchema.ColumnOptions.Rows[i]["OPTION_VALUE"], schema.ColumnOptions.Rows[i]["OPTION_VALUE"]);
            }
            
            AssertDataTablesEqual(initialSchema.Tables, schema.Tables);
            AssertDataTablesEqual(initialSchema.Columns, schema.Columns);
            AssertDataTablesEqual(initialSchema.ColumnOptions, schema.ColumnOptions);
            AssertDataTablesEqual(initialSchema.Indexes, schema.Indexes);
            AssertDataTablesEqual(initialSchema.IndexColumns, schema.IndexColumns);
            AssertDataTablesEqual(initialSchema.ReferentialConstraints, schema.ReferentialConstraints);
        }

        private static void AssertDataTablesEqual(DataTable expected, DataTable actual)
        {
            Assert.Equal(expected.Rows.Count, actual.Rows.Count);
            for (var row = 0; row < expected.Rows.Count; row++)
            {
                for (var col = 0; col < expected.Columns.Count; col++)
                {
                    if (Equals("IS_NULL_FILTERED", actual.Columns[col].ColumnName) ||
                        Equals("IS_NULLABLE", actual.Columns[col].ColumnName) && Equals("IndexColumns", actual.TableName))
                    {
                        // Ignore any differences in IS_NULL_FILTERED, as there is no way to generate a null-filtered index
                        // using NHibernate.
                    }
                    else
                    {
                        Assert.Equal(expected.Rows[row][col], actual.Rows[row][col]);
                    }
                }
            }
        }
    }
}
