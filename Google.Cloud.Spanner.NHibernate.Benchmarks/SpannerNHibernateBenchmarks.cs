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

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using Google.Api.Gax;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.NHibernate.IntegrationTests;
using Google.Cloud.Spanner.NHibernate.IntegrationTests.SampleEntities;
using NHibernate;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Google.Cloud.Spanner.NHibernate.Benchmarks
{

    public class SpannerNHibernateBenchmarks
    {
        private SpannerSampleFixture _fixture;

        private string _connectionString;

        private string _singerId;

        private static List<Singer> DataReaderToSingersList(DbDataReader reader)
        {
            var result = new List<Singer>();
            while (reader.Read())
            {
                result.Add(RowToSinger(reader));
            }
            return result;
        }

        private static Singer RowToSinger(DbDataReader reader)
        {
            var id = reader.GetString(reader.GetOrdinal("Id"));
            var firstName = reader.GetString(reader.GetOrdinal("FirstName"));
            var lastName = reader.GetString(reader.GetOrdinal("LastName"));
            var fullName = reader.GetString(reader.GetOrdinal("FullName"));
            var birthDate = reader.IsDBNull(reader.GetOrdinal("BirthDate")) ? new DateTime() : reader.GetDateTime(reader.GetOrdinal("BirthDate"));
            var picture = reader.IsDBNull(reader.GetOrdinal("Picture")) ? null : reader.GetFieldValue<byte[]>(reader.GetOrdinal("Picture"));
            return new Singer { Id = id, FirstName = firstName, LastName = lastName, FullName = fullName, BirthDate = SpannerDate.FromDateTime(birthDate), Picture = picture };
        }

        [GlobalSetup]
        public void SetupServer()
        {
            _fixture = new SpannerSampleFixture();
            _connectionString = _fixture.ConnectionString;
            using var connection = CreateConnection();
            _singerId = MaybeCreateSingerSpanner(connection);
            MaybeInsert100Singers(connection);
        }

        [GlobalCleanup]
        public void TeardownServer()
        {
            _fixture?.DisposeAsync().WaitWithUnwrappedExceptions();
        }

        private SpannerConnection CreateConnection() => new SpannerConnection(_connectionString);

        private static string MaybeCreateSingerSpanner(SpannerConnection connection)
        {
            var singerId = Guid.NewGuid().ToString();
            connection.RunWithRetriableTransaction(transaction =>
            {
                using var command = connection.CreateDmlCommand("INSERT INTO Singers (Id, FirstName, LastName) VALUES (@id, 'Test', 'Test')", new SpannerParameterCollection
                {
                    new SpannerParameter("id", SpannerDbType.String, singerId)
                });
                command.Transaction = transaction;
                return command.ExecuteNonQuery();
            });
            return singerId;
        }

        private Singer MaybeCreateSingerNHibernate(ISession session)
        {
            var singer = new Singer
            {
                FirstName = "Test",
                LastName = "Test",
            };
            session.Save(singer);
            session.Flush();
            return singer;
        }

        private void MaybeInsert100Singers(SpannerConnection connection)
        {
            var random = new Random();
            connection.RunWithRetriableTransaction(transaction =>
            {
                var command = transaction.CreateBatchDmlCommand();
                for (var row = 0; row < 100; row++)
                {
                    var singerId = Guid.NewGuid().ToString();
                    var date = new SpannerDate(random.Next(1900, 2020), random.Next(1, 13), random.Next(1, 29));
                    var firstName = _fixture.RandomString(10);
                    var lastName = _fixture.RandomString(15);
                    var picture = new byte[random.Next(1, 4097)];
                    random.NextBytes(picture);
                    command.Add("INSERT INTO Singers (Id, FirstName, LastName, BirthDate, Picture) VALUES (@id, @firstName, @lastName, @birthDate, @picture)", new SpannerParameterCollection
                    {
                        new SpannerParameter("id", SpannerDbType.String, singerId),
                        new SpannerParameter("firstName", SpannerDbType.String, firstName),
                        new SpannerParameter("lastName", SpannerDbType.String, lastName),
                        new SpannerParameter("birthDate", SpannerDbType.Date, date),
                        new SpannerParameter("picture", SpannerDbType.Bytes, picture)
                    });
                }
                command.ExecuteNonQuery();
            });
        }

        [Benchmark]
        public Singer ReadOneRowSpanner()
        {
            using var connection = CreateConnection();
            using var command = connection.CreateSelectCommand("SELECT * FROM Singers WHERE Id=@id", new SpannerParameterCollection {
                new SpannerParameter("id", SpannerDbType.String, _singerId)
            });
            using var reader = command.ExecuteReader();
            return reader.Read() ? RowToSinger(reader) : null;
        }

        [Benchmark]
        public Singer ReadOneRowNHibernate()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singer = session.Load<Singer>(_singerId);
            // Make sure that we are returning an actual Singer instance and not a proxy.
            if (singer.FullName == null)
            {
                throw new InvalidProgramException();
            }
            return singer;
        }

        [Benchmark]
        public string SaveOneRowWithFetchAfterSpanner()
        {
            using var connection = CreateConnection();
            var singerId = Guid.NewGuid().ToString();
            return connection.RunWithRetriableTransaction(transaction =>
            {
                var command = transaction.CreateBatchDmlCommand();
                command.Add("INSERT INTO Singers (Id, FirstName, LastName, BirthDate, Picture) VALUES (@id, @firstName, @lastName, @birthDate, @picture)", new SpannerParameterCollection
                {
                    new SpannerParameter("id", SpannerDbType.String, singerId),
                    new SpannerParameter("firstName", SpannerDbType.String, "Pete"),
                    new SpannerParameter("lastName", SpannerDbType.String, "Allison"),
                    new SpannerParameter("birthDate", SpannerDbType.Date, new DateTime(1998, 10, 6)),
                    new SpannerParameter("picture", SpannerDbType.Bytes, new byte[] { 1, 2, 3 }),
                });
                command.ExecuteNonQuery();
                
                var selectCommand = connection.CreateSelectCommand("SELECT FullName FROM Singers WHERE Id=@id", new SpannerParameterCollection
                {
                    new SpannerParameter("id", SpannerDbType.String, singerId),
                });
                selectCommand.Transaction = transaction;
                var fullName = selectCommand.ExecuteScalar();
                if (!"Pete Allison".Equals(fullName))
                {
                    throw new InvalidProgramException($"Received invalid full name: {fullName}");
                }
                return singerId;
            });
        }

        [Benchmark]
        public string SaveOneRowWithFetchAfterNHibernate()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singer = new Singer
            {
                FirstName = "Pete",
                LastName = "Allison",
                BirthDate = new SpannerDate(1998, 10, 6),
                Picture = new byte[] { 1, 2, 3 },
            };
            var id = (string) session.Save(singer);
            // This will flush the change to Spanner and commit the transaction, as there is no explicit transaction.
            session.Flush();
            if (singer.FullName != "Pete Allison")
            {
                throw new InvalidProgramException();
            }
            return id;
        }

        [Benchmark]
        public long SaveMultipleRowsUsingMutationsSpanner()
        {
            using var connection = CreateConnection();
            var singerId = MaybeCreateSingerSpanner(connection);
            return connection.RunWithRetriableTransaction(transaction =>
            {
                var updateCount = 0;
                for (var row = 0; row < 100; row++)
                {
                    var command = connection.CreateInsertCommand("Albums", new SpannerParameterCollection
                    {
                        new SpannerParameter("Id", SpannerDbType.String, Guid.NewGuid().ToString()),
                        new SpannerParameter("Title", SpannerDbType.String, $"Album{row}{Guid.NewGuid().ToString()}"),
                        new SpannerParameter("ReleaseDate", SpannerDbType.Date, new DateTime(1998, 10, 6)),
                        new SpannerParameter("Singer", SpannerDbType.String, singerId),
                    });
                    command.Transaction = transaction;
                    updateCount += command.ExecuteNonQuery();
                }
                return updateCount;
            });
        }

        [Benchmark]
        public long SaveMultipleRowsUsingMutationsNHibernate()
        {
            const int rowCount = 100;
            using var session = _fixture.SessionFactory.OpenSession();
            var singer = MaybeCreateSingerNHibernate(session);
            using var transaction = session.BeginTransaction(MutationUsage.Always);
            for (var row = 0; row < rowCount; row++)
            {
                session.Save(new Album
                {
                    Title = $"Album{row}{Guid.NewGuid().ToString()}",
                    ReleaseDate = new SpannerDate(1998, 10, 6),
                    Singer = singer,
                });
            }
            transaction.Commit();
            
            return rowCount;
        }

        [Benchmark]
        public long SaveMultipleRowsUsingDmlSpanner()
        {
            const int rowCount = 100;
            using var connection = CreateConnection();
            var singer = MaybeCreateSingerSpanner(connection);
            return connection.RunWithRetriableTransaction(transaction =>
            {
                var command = transaction.CreateBatchDmlCommand();
                for (var row = 0; row < rowCount; row++)
                {
                    command.Add("INSERT INTO Albums (Id, Title, ReleaseDate, Singer) VALUES (@id, @title, @releaseDate, @singer)", new SpannerParameterCollection
                    {
                        new SpannerParameter("Id", SpannerDbType.String, Guid.NewGuid().ToString()),
                        new SpannerParameter("Title", SpannerDbType.String, $"Album{row}"),
                        new SpannerParameter("ReleaseDate", SpannerDbType.Date, new DateTime(1998, 10, 6)),
                        new SpannerParameter("Singer", SpannerDbType.String, singer),
                    });
                }
                return command.ExecuteNonQuery().Sum();
            });
        }

        [Benchmark]
        public long SaveMultipleRowsUsingDmlNHibernate()
        {
            const int rowCount = 100;
            using var session = _fixture.SessionFactory.OpenSession();
            var singer = MaybeCreateSingerNHibernate(session);
            using var transaction = session.BeginTransaction();
            for (var row = 0; row < rowCount; row++)
            {
                session.Save(new Album
                {
                    Title = $"Album{row}",
                    ReleaseDate = new SpannerDate(1998, 10, 6),
                    Singer = singer,
                });
            }
            transaction.Commit();
            
            return rowCount;
        }

        [Benchmark]
        public List<Singer> SelectMultipleSingersSpanner()
        {
            using var connection = CreateConnection();
            using var command = connection.CreateSelectCommand("SELECT * FROM Singers ORDER BY LastName");
            using var reader = command.ExecuteReader();
            return DataReaderToSingersList(reader);
        }

        [Benchmark]
        public List<Singer> SelectMultipleSingersNHibernate()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            return session
                .Query<Singer>()
                .OrderBy(s => s.LastName)
                .ToList();
        }

        [Benchmark]
        public List<Singer> SelectMultipleSingersInReadOnlyTransactionSpanner()
        {
            using var connection = CreateConnection();
            using var transaction = connection.BeginReadOnlyTransaction();
            using var command = connection.CreateSelectCommand("SELECT * FROM Singers ORDER BY LastName");
            command.Transaction = transaction;
            using var reader = command.ExecuteReader();
            return DataReaderToSingersList(reader);
        }

        [Benchmark]
        public List<Singer> SelectMultipleSingersInReadOnlyTransactionNHibernate()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginReadOnlyTransaction();
            return session
                .Query<Singer>()
                .OrderBy(s => s.LastName)
                .ToList();
        }

        [Benchmark]
        public List<Singer> SelectMultipleSingersInReadWriteTransactionSpanner()
        {
            using var connection = CreateConnection();
            return connection.RunWithRetriableTransaction(transaction =>
            {
                using var command = connection.CreateSelectCommand("SELECT * FROM Singers ORDER BY LastName");
                command.Transaction = transaction;
                using var reader = command.ExecuteReader();
                var singers = DataReaderToSingersList(reader);
                return singers;
            });
        }

        [Benchmark]
        public List<Singer> SelectMultipleSingersInReadWriteTransactionNHibernate()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            var singers = session
                .Query<Singer>()
                .OrderBy(s => s.LastName)
                .ToList();
            transaction.Commit();
            return singers;
        }
    }

    public static class BenchmarkProgram
    {
        public static void Main(string[] args)
        {
            BenchmarkRunner.Run<SpannerNHibernateBenchmarks>(DefaultConfig.Instance.WithOption(ConfigOptions.DisableOptimizationsValidator, true));
        }
    }
}
