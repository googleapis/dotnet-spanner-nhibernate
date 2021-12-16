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

using Google.Cloud.Spanner.NHibernate.Samples.SampleModel;
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
            var cmd = session.Connection.CreateCommand();
            cmd.CommandText =
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='' AND TABLE_SCHEMA=''";
            var numberOfTables = (long) await cmd.ExecuteScalarAsync();
            
            // Create a SpannerSchemaExporter and use this to drop and re-create the database schema from the entity
            // model.
            var exporter = new SpannerSchemaExport(configuration.Configuration);
            // This will automatically execute a drop-and-recreate script and write the statements that are executed to
            // stdout.
            try
            {
                await exporter.CreateAsync(true, true);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}