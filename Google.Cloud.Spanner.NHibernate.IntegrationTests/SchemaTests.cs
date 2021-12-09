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
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests
{
    [Collection(nameof(NonParallelTestCollection))]
    public class SchemaTests : IClassFixture<SpannerSampleFixture>
    {
        private readonly SpannerSampleFixture _fixture;

        public SchemaTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [Fact]
        public void CanDropAndRecreateSchema()
        {
            using var connection = new SpannerRetriableConnection(_fixture.GetConnection());
            var initialTables = connection.GetSchema("Tables");
            var initialColumns = connection.GetSchema("Columns");
            var initialColumnOptions = connection.GetSchema("ColumnOptions");
            
            var exporter = new SpannerSchemaExport(_fixture.Configuration);
            exporter.Execute(false, true, false);
            
            var tables = connection.GetSchema("Tables");
            var columns = connection.GetSchema("Columns");
            var columnOptions = connection.GetSchema("ColumnOptions");
            
            Assert.Equal(initialTables.Rows.Count, tables.Rows.Count);
            for (var i = 0; i < initialTables.Rows.Count; i++)
            {
                Assert.Equal(initialTables.Rows[i]["TABLE_NAME"], tables.Rows[i]["TABLE_NAME"]);
            }
            
            Assert.Equal(initialColumns.Rows.Count, columns.Rows.Count);
            for (var i = 0; i < initialColumns.Rows.Count; i++)
            {
                Assert.Equal(initialColumns.Rows[i]["TABLE_NAME"], columns.Rows[i]["TABLE_NAME"]);
                Assert.Equal(initialColumns.Rows[i]["COLUMN_NAME"], columns.Rows[i]["COLUMN_NAME"]);
                Assert.Equal(initialColumns.Rows[i]["IS_NULLABLE"], columns.Rows[i]["IS_NULLABLE"]);
                Assert.Equal(initialColumns.Rows[i]["SPANNER_TYPE"], columns.Rows[i]["SPANNER_TYPE"]);
                Assert.Equal(initialColumns.Rows[i]["COLUMN_DEFAULT"], columns.Rows[i]["COLUMN_DEFAULT"]);
                Assert.Equal(initialColumns.Rows[i]["IS_GENERATED"], columns.Rows[i]["IS_GENERATED"]);
                Assert.Equal(initialColumns.Rows[i]["GENERATION_EXPRESSION"], columns.Rows[i]["GENERATION_EXPRESSION"]);
            }
            
            Assert.Equal(initialColumnOptions.Rows.Count, columnOptions.Rows.Count);
            for (var i = 0; i < initialColumnOptions.Rows.Count; i++)
            {
                Assert.Equal(initialColumnOptions.Rows[i]["TABLE_NAME"], columnOptions.Rows[i]["TABLE_NAME"]);
                Assert.Equal(initialColumnOptions.Rows[i]["COLUMN_NAME"], columnOptions.Rows[i]["COLUMN_NAME"]);
                Assert.Equal(initialColumnOptions.Rows[i]["OPTION_NAME"], columnOptions.Rows[i]["OPTION_NAME"]);
                Assert.Equal(initialColumnOptions.Rows[i]["OPTION_TYPE"], columnOptions.Rows[i]["OPTION_TYPE"]);
                Assert.Equal(initialColumnOptions.Rows[i]["OPTION_VALUE"], columnOptions.Rows[i]["OPTION_VALUE"]);
            }
        }
    }
}