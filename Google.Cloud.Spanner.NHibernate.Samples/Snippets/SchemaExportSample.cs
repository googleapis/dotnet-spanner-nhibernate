// Copyright 2021 Google Inc. All Rights Reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using NHibernate.Tool.hbm2ddl;
using System;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// Sample showing how to use <see cref="SchemaExport"/> with Google Cloud Spanner.
    ///
    /// The standard NHibernate <see cref="SchemaExport"/> is not compatible with the DDL dialect of Cloud Spanner.
    /// The Cloud Spanner driver therefore comes with its own implementation in <see cref="SpannerSchemaExport"/>. The
    /// SpannerSchemaExport generates DDL that is compatible with Cloud Spanner, and automatically executes multiple DDL
    /// statements as one batch, which significantly improves execution speed for large database schemas.
    /// 
    /// Run from the command line with `dotnet run SchemaExport`
    /// </summary>
    public static class SchemaExportSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();
            
            // The sample runner automatically creates the data model that is required for the entities that are used
            // by the samples in this project. The following query gets the number of tables currently in the database.
            // The SQL script that is used to create the data model can be found in `SampleModel/SampleDataModel.sql`.
            var cmd = session.Connection.CreateCommand();
            cmd.CommandText =
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='' AND TABLE_SCHEMA=''";
            var numberOfTablesBeforeDropAndRecreate = (long) await cmd.ExecuteScalarAsync();
            
            // Create a SpannerSchemaExporter and use this to drop and re-create the database schema from the entity
            // model. This should result in the exact same data model as the SQL statements in `SampleDataModel.sql`.
            // The mapping model must include all metadata for the schema, such as which columns are not-nullable, the
            // length of STRING columns, etc.
            // Tables are created in the order that they are added to the mapping. Tables are dropped in the opposite
            // order. This means that if the model includes interleaved tables, the parent table must be added BEFORE
            // the child table. See SampleConfiguration.cs for an example.
            var exporter = new SpannerSchemaExport(configuration.Configuration);
            // This will automatically execute a drop-and-recreate script and print the statements that are executed to
            // the console (StdOut).
            await exporter.CreateAsync(true /*useStdOut*/, true /*execute*/);
            
            cmd.CommandText =
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='' AND TABLE_SCHEMA=''";
            var numberOfTablesAfterDropAndRecreate = (long) await cmd.ExecuteScalarAsync();
            
            Console.WriteLine();
            Console.WriteLine("Dropped and recreated the schema based on the NHibernate mapping");
            Console.WriteLine($"Tables BEFORE drop-and-recreate: {numberOfTablesBeforeDropAndRecreate}");
            Console.WriteLine($"Tables AFTER drop-and-recreate: {numberOfTablesAfterDropAndRecreate}");
        }
    }
}