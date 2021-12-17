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

using Google.Api.Gax;
using Google.Cloud.Spanner.NHibernate.IntegrationTests.InterleavedTableTests.Entities;
using Google.Cloud.Spanner.V1.Internal.Logging;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Mapping.ByCode;
using NHibernate.Util;
using System;
using System.IO;
using System.Reflection;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests.InterleavedTableTests
{
    /// <summary>
    /// Test fixture using a data model with three hierarchical tables (Singers => Albums => Tracks).
    /// </summary>
    public class SpannerInterleavedTableFixture : SpannerFixtureBase
    {
        public SpannerInterleavedTableFixture()
        {
            if (Database.Fresh)
            {
                Logger.DefaultLogger.Debug($"Creating database {Database.DatabaseName}");
                CreateTables();
            }
            else
            {
                Logger.DefaultLogger.Debug($"Deleting data in {Database.DatabaseName}");
                ClearTables();
            }
            Logger.DefaultLogger.Debug($"Ready to run tests");
            ReflectHelper.ClassForName(typeof(SpannerDriver).AssemblyQualifiedName);
            Configuration = new Configuration().DataBaseIntegration(db =>
            {
                db.Dialect<SpannerDialect>();
                db.ConnectionString = ConnectionString;
                db.BatchSize = 100;
            });
            var mapper = new ModelMapper();
            mapper.AddMapping<SingerMapping>();
            mapper.AddMapping<AlbumMapping>();
            mapper.AddMapping<TrackMapping>();
            var mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            Configuration.AddMapping(mapping);
            
            SessionFactory = Configuration.BuildSessionFactory();
        }
        
        public Configuration Configuration { get; }
        
        public ISessionFactory SessionFactory { get; }

        private void ClearTables()
        {
            using var con = GetConnection();
            con.RunWithRetriableTransactionAsync((transaction) =>
            {
                var cmd = transaction.CreateBatchDmlCommand();
                foreach (var table in new string[]
                {
                    "Tracks",
                    "Albums",
                    "Singers",
                })
                {
                    cmd.Add($"DELETE FROM {table} WHERE TRUE");
                }
                return cmd.ExecuteNonQueryAsync();
            }).ResultWithUnwrappedExceptions();
        }

        /// <summary>
        /// Creates the sample data model. This method is only called when a new database has been
        /// created.
        /// </summary>
        private void CreateTables()
        {
            var dirPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            var sampleModel = "InterleavedTableTests/InterleavedTableDataModel.sql";
            var fileName = Path.Combine(dirPath, sampleModel);
            var script = File.ReadAllText(fileName);
            var statements = script.Split(";");
            for (var i = 0; i < statements.Length; i++)
            {
                // Remove license header from script
                if (statements[i].IndexOf("/*") >= 0 && statements[i].IndexOf("*/") >= 0)
                {
                    int startIndex = statements[i].IndexOf("/*");
                    int endIndex = statements[i].IndexOf("*/", startIndex) + "*/".Length;
                    statements[i] = statements[i].Remove(startIndex, endIndex - startIndex);
                }
                statements[i] = statements[i].Trim(new char[] { '\r', '\n' });
            }
            int length = statements.Length;
            if (statements[length - 1] == "")
            {
                length--;
            }
            ExecuteDdl(statements, length);
        }

        private void ExecuteDdl(string[] ddl, int length)
        {
            string[] extraStatements = new string[length - 1];
            Array.Copy(ddl, 1, extraStatements, 0, extraStatements.Length);
            using var connection = GetConnection();
            connection.CreateDdlCommand(ddl[0].Trim(), extraStatements).ExecuteNonQuery();
        }
    }
}
