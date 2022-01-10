// Copyright 2022 Google Inc. All Rights Reserved.
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

using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.NHibernate.Samples.SampleModel;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using NHibernate.Cfg;
using NHibernate.Linq;
using NHibernate.Mapping.ByCode;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Environment = NHibernate.Cfg.Environment;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// This sample shows how to map all Cloud Spanner data types to an NHibernate type.
    /// 
    /// Run from the command line with `dotnet run DataTypesSample`
    /// </summary>
    public static class DataTypesSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            // Create a sample table that contains all data types.
            await CreateTable(configuration);
            // Create a session factory that also includes the AllTypesEntity.

            // Create an NHibernate configuration that will use the Spanner dialect and driver.
            var allTypesConfiguration = new Configuration().DataBaseIntegration(db =>
            {
                db.Dialect<SpannerDialect>();
                db.ConnectionString = configuration.Configuration.GetProperty(Environment.ConnectionString);
            });
            // Create a mapper that will include the AllTypesEntity and create an NHibernate mapping from it.
            var mapper = new ModelMapper();
            mapper.AddMapping<AllTypesEntityMapping>();
            var mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            allTypesConfiguration.AddMapping(mapping);

            // Create a session factory and open a session.
            var sessionFactory = allTypesConfiguration.BuildSessionFactory();
            using var session = sessionFactory.OpenSession();

            try
            {
                // Create and save a record with all data types.
                var transaction = session.BeginTransaction();
                var record = new AllTypesEntity
                {
                    ColInt64 = 1L,
                    ColBool = true,
                    ColBytes = new byte[] { 1, 2, 3 },
                    ColDate = new SpannerDate(2022, 1, 3),
                    ColFloat64 = 3.14d,
                    ColJson = new SpannerJson("{}"),
                    ColNumeric = new SpannerNumeric(V1.SpannerNumeric.FromDecimal(3.14m, LossOfPrecisionHandling.Throw)),
                    ColString = "Hello World!",
                    ColTimestamp = DateTime.Now,
                    ColInt64Array = new SpannerInt64Array(new List<long?> { 1L, 2L, 3L }),
                    ColBoolArray = new SpannerBoolArray(new List<bool?> { true, false, true }),
                    ColBytesArray = new SpannerBytesArray(new List<byte[]>
                        { new byte[] { 1, 2, 3 }, new byte[] { 3, 2, 1 } }),
                    ColDateArray = new SpannerDateArray(new List<DateTime?> { DateTime.Today, new DateTime(2022, 1, 3) }),
                    ColFloat64Array = new SpannerFloat64Array(new List<double?> { 3.14d, double.MaxValue, double.NaN }),
                    ColJsonArray = new SpannerJsonArray(new List<string> { "{}", "[]" }),
                    ColNumericArray = new SpannerNumericArray(new List<V1.SpannerNumeric?>
                        { V1.SpannerNumeric.MaxValue, V1.SpannerNumeric.Zero }),
                    ColStringArray = new SpannerStringArray(new List<string> { "String 1", "String 2" }),
                    ColTimestampArray = new SpannerTimestampArray(new List<DateTime?>
                        { DateTime.Now, new DateTime(2022, 1, 3, 10, 54, 10) }),
                };
                await session.SaveAsync(record);
                await transaction.CommitAsync();
                Console.WriteLine("Saved entity with all data types");
                Console.WriteLine("");

                Console.WriteLine("Querying for rows with all data types");
                Console.WriteLine("");
                // Query the database for the record that we just saved.
                var result = await session
                    .Query<AllTypesEntity>()
                    .Where(r => r.ColString.Equals("Hello World!"))
                    .ToListAsync();
                result.ForEach(row => Console.WriteLine($"Found: {row.ColString}"));
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        private static async Task CreateTable(SampleConfiguration configuration)
        {
            using var connection = new SpannerRetriableConnection(new SpannerConnection(
                configuration.Configuration.GetProperty(Environment.ConnectionString), ChannelCredentials.Insecure));
            const string sql = @"CREATE TABLE TableWithAllColumnTypes (
                ColInt64 INT64 NOT NULL,
                ColFloat64 FLOAT64,
                ColNumeric NUMERIC,
                ColBool BOOL,
                ColString STRING(100),
                ColBytes BYTES(100),
                ColJson JSON,
                ColDate DATE,
                ColTimestamp TIMESTAMP,
                ColCommitTs TIMESTAMP OPTIONS (allow_commit_timestamp=true),
                ColInt64Array ARRAY<INT64>,
                ColFloat64Array ARRAY<FLOAT64>,
                ColNumericArray ARRAY<NUMERIC>,
                ColBoolArray ARRAY<BOOL>,
                ColStringArray ARRAY<STRING(100)>,
                ColBytesArray ARRAY<BYTES(100)>,
                ColJsonArray ARRAY<JSON>,
                ColDateArray ARRAY<DATE>,
                ColTimestampArray ARRAY<TIMESTAMP>,
                ColComputed STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED,
            ) PRIMARY KEY (ColInt64)";
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}