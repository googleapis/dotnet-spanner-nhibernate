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
using Google.Cloud.Spanner.NHibernate.IntegrationTests.SampleEntities;
using Google.Cloud.Spanner.V1.Internal.Logging;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Mapping.ByCode;
using NHibernate.Util;
using System;
using System.IO;
using System.Reflection;
using Environment = NHibernate.Cfg.Environment;
using PropertyGeneration = NHibernate.Mapping.PropertyGeneration;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests
{
    /// <summary>
    /// Base classes for test fixtures using the sample data model.
    /// If TEST_SPANNER_DATABASE is set to an existing database, that database will be used and the
    /// fixture assumes that the database already contains the sample data model. Any data in the
    /// existing database will be deleted.
    /// 
    /// Otherwise a new database with the sample data model is automatically created and used. The
    /// generated database is dropped when the fixture is disposed.
    /// </summary>
    public class SpannerSampleFixture : SpannerFixtureBase
    {
        public SpannerSampleFixture()
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
            mapper.AddMapping<AlbumMapping>();
            mapper.AddMapping<ConcertMapping>();
            mapper.AddMapping<PerformanceMapping>();
            mapper.AddMapping<SingerMapping>();
            mapper.AddMapping<TrackMapping>();
            mapper.AddMapping<VenueMapping>();

            mapper.AddMapping<TableWithAllColumnTypesMapping>();
            mapper.AddMapping<SingerWithVersionMapping>();
            mapper.AddMapping<AlbumWithVersionMapping>();
            var mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            Configuration.AddMapping(mapping);
            
            // This is needed for support for query hints.
            Configuration.SetInterceptor(new SpannerQueryHintInterceptor());
            Configuration.Properties[Environment.UseSqlComments] = "true";
            
            SessionFactory = Configuration.BuildSessionFactory();

            // Configure some entities to use mutations instead of DML in a separate SessionFactory.
            // Disable property generation when we are using mutations, as the value cannot be read before everything
            // has been committed.
            Configuration.GetClassMapping(typeof(Singer)).GetProperty(nameof(Singer.FullName)).Generation = PropertyGeneration.Never;
            Configuration.GetClassMapping(typeof(TableWithAllColumnTypes)).GetProperty(nameof(TableWithAllColumnTypes.ColComputed)).Generation = PropertyGeneration.Never;
            SessionFactoryForMutations = Configuration.BuildSessionFactory();
        }
        
        public Configuration Configuration { get; }
        
        public ISessionFactory SessionFactory { get; }
        
        public ISessionFactory SessionFactoryForMutations { get; }

        private void ClearTables()
        {
            using var con = GetConnection();
            con.RunWithRetriableTransactionAsync((transaction) =>
            {
                var cmd = transaction.CreateBatchDmlCommand();
                foreach (var table in new string[]
                {
                    "AlbumsWithVersion",
                    "SingersWithVersion",
                    "TableWithAllColumnTypes",
                    "Performances",
                    "Concerts",
                    "Venues",
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
            // We must use a slightly edited sample data model for the emulator, as the emulator does not support:
            // 1. JSON data type.
            // 2. Check constraints.
            // 3. Computed columns that are not the last column in the table.
            var sampleModel = IsEmulator ? "SampleDataModel - Emulator.sql" : "SampleDataModel.sql";
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
