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
using Google.Cloud.Spanner.Connection.MockServer;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.NHibernate.Tests.Entities;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using NHibernate.Criterion;
using NHibernate.Linq;
using NHibernate.SqlCommand;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using IsolationLevel = System.Data.IsolationLevel;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    public class SpannerDriverTest : IClassFixture<NHibernateMockServerFixture>
    {
        private readonly NHibernateMockServerFixture _fixture;

        public SpannerDriverTest(NHibernateMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        [Fact]
        public async Task GetSingerAsync_ReturnsNull_IfNotFound()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            AddEmptySingerResult(GetSelectSingerSql());

            var singer = await session.GetAsync<Singer>(1L);
            Assert.Null(singer);
        }

        [Fact]
        public async Task GetSingerAsync_ReturnsNotNull_IfFound()
        {
            using var session = _fixture.SessionFactory.OpenSession();

            var sql = AddSingerResult(GetSelectSingerSql());
            var singer = await session.GetAsync<Singer>(1L);
            Assert.NotNull(singer);
            Assert.Equal("Alice", singer.FirstName);
            Assert.Equal(1L, singer.SingerId);
            Assert.Equal(new SpannerDate(1998, 5, 12), singer.BirthDate);
            Assert.Equal("Alice", singer.FirstName);
            Assert.Equal("Alice Morrison", singer.FullName);
            Assert.Equal("Morrison", singer.LastName);
            Assert.Equal(new byte[]{1,2,3}, singer.Picture);

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Null(request.Transaction);
                }
            );
            // A read-only operation should not initiate and commit a transaction.
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<V1.CommitRequest>());
        }

        [Fact]
        public async Task CanGetListOfAlbumsFromSinger()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerSql = AddSingerResult(GetSelectSingerSql());
            var albumsSql = AddSingerAlbumsResults(GetSelectSingerAlbumsSql(), new []
            {
                new object[] { 1L, 1L, 1L, "Title 1", null, 1L },
                new object[] { 1L, 2L, 2L, "Title 2", null, 1L },
            });
            
            var singer = await session.GetAsync<Singer>(1L);
            var albums = singer.Albums;
            
            Assert.Collection(albums,
                album =>
                {
                    Assert.Equal(1L, album.AlbumId);
                    Assert.Equal("Title 1", album.Title);
                },
                album =>
                {
                    Assert.Equal(2L, album.AlbumId);
                    Assert.Equal("Title 2", album.Title);
                }
            );
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(singerSql, request.Sql);
                    Assert.Null(request.Transaction);
                },
                request =>
                {
                    Assert.Equal(albumsSql, request.Sql);
                    Assert.Null(request.Transaction);
                }
            );
            // A read-only operation should not initiate and commit a transaction.
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<CommitRequest>());
        }

        [Fact]
        public async Task InsertMultipleSingers_UsesSameTransaction()
        {
            var insertSql = "INSERT INTO Singer (FirstName, LastName, BirthDate, Picture, SingerId) VALUES (@p0, @p1, @p2, @p3, @p4)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            var selectFullNameSql = AddSelectSingerFullNameResult("Alice Morrison", 0);

            using var session = _fixture.SessionFactory.OpenSession();
            var transaction = session.BeginTransaction();
            
            await session.SaveAsync(new Singer
            {
                SingerId = 1L,
                FirstName = "Alice",
                LastName = "Morrison",
            });
            await session.SaveAsync(new Singer
            {
                SingerId = 2L,
                FirstName = "Alice",
                LastName = "Morrison",
            });
            await transaction.CommitAsync();

            var transactionId = _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>().First().Transaction.Id;
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(insertSql, request.Sql);
                    Assert.Equal(transactionId, request.Transaction.Id);
                },
                request =>
                {
                    Assert.Equal(selectFullNameSql, request.Sql);
                    Assert.Equal(transactionId, request.Transaction.Id);
                },
                request =>
                {
                    Assert.Equal(insertSql, request.Sql);
                    Assert.Equal(transactionId, request.Transaction.Id);
                },
                request =>
                {
                    Assert.Equal(selectFullNameSql, request.Sql);
                    Assert.Equal(transactionId, request.Transaction.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));
        }

        [Fact]
        public async Task InsertSinger_SelectsFullName()
        {
            using var session = _fixture.SessionFactory.OpenSession();

            var insertSql = "INSERT INTO Singer (FirstName, LastName, BirthDate, Picture, SingerId) VALUES (@p0, @p1, @p2, @p3, @p4)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            var selectFullNameSql = AddSelectSingerFullNameResult("Alice Morrison", 0);

            var singer = new Singer
            {
                SingerId = 1L,
                FirstName = "Alice",
                LastName = "Morrison",
            };
            var id = await session.SaveAsync(singer);
            await session.FlushAsync();

            Assert.Equal(1L, id);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(insertSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(selectFullNameSql, request.Sql);
                    Assert.Null(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));

            Assert.Collection(_fixture.SpannerMock.Requests
                .Where(request => request is V1.CommitRequest || request is V1.ExecuteSqlRequest)
                .Select(request => request.GetType()),
                request => Assert.Equal(typeof(V1.ExecuteSqlRequest), request),
                request => Assert.Equal(typeof(V1.CommitRequest), request),
                request => Assert.Equal(typeof(V1.ExecuteSqlRequest), request)
            );
        }

        [Fact]
        public async Task UpdateSinger_SelectsFullName()
        {
            // Setup results.
            var updateSql = "UPDATE Singer SET FirstName = @p0, LastName = @p1, BirthDate = @p2, Picture = @p3 WHERE SingerId = @p4";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));
            var selectSingerSql = AddSingerResult(GetSelectSingerSql());
            var selectFullNameSql = AddSelectSingerFullNameResult("Alice Pieterson-Morrison", 0);

            using var session = _fixture.SessionFactory.OpenSession();
            var singer = await session.GetAsync<Singer>(1L);
            singer.LastName = "Pieterson-Morrison";
            await session.FlushAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(selectSingerSql, request.Sql);
                    Assert.Null(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(updateSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(selectFullNameSql, request.Sql);
                    Assert.Null(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));
        }

        [Fact]
        public async Task DeleteSinger_DoesNotSelectFullName()
        {
            // Setup results.
            var updateSql = "UPDATE Album SET Singer = null WHERE Singer = @p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(0L));
            var deleteSql = $"DELETE FROM Singer WHERE SingerId = @p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(deleteSql, StatementResult.CreateUpdateCount(1L));
            var selectSingerSql = AddSingerResult(GetSelectSingerSql());

            using var session = _fixture.SessionFactory.OpenSession();
            var singer = await session.GetAsync<Singer>(1L);
            await session.DeleteAsync(singer);
            await session.FlushAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(selectSingerSql, request.Sql);
                    Assert.Null(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(updateSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(deleteSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            // The update and the delete are done in separate transactions, as there is no explicit transaction defined.
            Assert.Equal(2, _fixture.SpannerMock.Requests.Count(request => request is CommitRequest));
        }

        [Fact]
        public async Task CanUseReadOnlyTransaction()
        {
            var sql = AddSingerResult(GetSelectSingerSql());
            using var session = _fixture.SessionFactory.OpenSession();
            using var connection = session.Connection as SpannerRetriableConnection;
            connection!.CreateReadOnlyTransactionForSnapshot = true;
            connection.ReadOnlyStaleness = TimestampBound.Strong;

            var transaction = session.BeginTransaction(IsolationLevel.Snapshot);
            var singer = await session.GetAsync<Singer>(1L);
            await transaction.CommitAsync();
            
            Assert.NotNull(singer);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests
                .OfType<V1.BeginTransactionRequest>()
                .Where(request => request.Options?.ReadOnly?.Strong ?? false));
        }

        [Fact]
        public async Task CanUseReadOnlyTransactionWithTimestampBound()
        {
            var sql = AddSingerResult(GetSelectSingerSql());
            using var session = _fixture.SessionFactory.OpenSession();
            using var connection = session.Connection as SpannerRetriableConnection;
            connection!.CreateReadOnlyTransactionForSnapshot = true;
            connection.ReadOnlyStaleness = TimestampBound.OfExactStaleness(TimeSpan.FromSeconds(10));

            var transaction = session.BeginTransaction(IsolationLevel.Snapshot);
            var singer = await session.GetAsync<Singer>(1L);
            await transaction.CommitAsync();
            
            Assert.NotNull(singer);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests
                .OfType<V1.BeginTransactionRequest>()
                .Where(request => request.Options?.ReadOnly?.ExactStaleness?.Seconds == 10L));
        }

        [Fact]
        public async Task CanReadWithMaxStaleness()
        {
            var sql = AddSingerResult(GetSelectSingerSql());
            using var session = _fixture.SessionFactory.OpenSession();
            using var connection = session.Connection as SpannerRetriableConnection;
            connection!.ReadOnlyStaleness = TimestampBound.OfMaxStaleness(TimeSpan.FromSeconds(10));
            var singer = await session.GetAsync<Singer>(1L);

            Assert.NotNull(singer);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal(Duration.FromTimeSpan(TimeSpan.FromSeconds(10)), request.Transaction?.SingleUse?.ReadOnly?.MaxStaleness);
                }
            );
        }

        [Fact]
        public async Task CanReadWithExactStaleness()
        {
            var sql = AddSingerResult(GetSelectSingerSql());
            using var session = _fixture.SessionFactory.OpenSession();
            using var connection = session.Connection as SpannerRetriableConnection;
            connection!.ReadOnlyStaleness = TimestampBound.OfExactStaleness(TimeSpan.FromSeconds(5.5));
            await session.GetAsync<Singer>(1L);

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal(Duration.FromTimeSpan(TimeSpan.FromSeconds(5.5)), request.Transaction?.SingleUse?.ReadOnly?.ExactStaleness);
                }
            );
        }

        [Fact]
        public async Task CanReadWithMinReadTimestamp()
        {
            var sql = AddSingerResult(GetSelectSingerSql());
            using var session = _fixture.SessionFactory.OpenSession();
            using var connection = session.Connection as SpannerRetriableConnection;
            connection!.ReadOnlyStaleness = TimestampBound.OfMinReadTimestamp(DateTime.Parse("2021-09-08T17:18:01.123+02:00").ToUniversalTime());
            await session.GetAsync<Singer>(1L);
            
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal(Timestamp.FromDateTime(new DateTime(2021, 9, 8, 15, 18, 1, 123, DateTimeKind.Utc)), request.Transaction?.SingleUse?.ReadOnly?.MinReadTimestamp);
                }
            );
        }

        [Fact]
        public async Task CanReadWithReadTimestamp()
        {
            var sql = AddSingerResult(GetSelectSingerSql());
            using var session = _fixture.SessionFactory.OpenSession();
            using var connection = session.Connection as SpannerRetriableConnection;
            connection!.ReadOnlyStaleness = TimestampBound.OfReadTimestamp(DateTime.Parse("2021-09-08T15:18:02Z").ToUniversalTime());
            await session.GetAsync<Singer>(1L);

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal(Timestamp.FromDateTime(new DateTime(2021, 9, 8, 15, 18, 2, DateTimeKind.Utc)), request.Transaction?.SingleUse?.ReadOnly?.ReadTimestamp);
                }
            );
        }

        [Fact]
        public async Task InsertUsingRawSqlReturnsUpdateCountWithoutAdditionalSelectCommand()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var today = SpannerDate.FromDateTime(DateTime.SpecifyKind(DateTime.Today, DateTimeKind.Unspecified));
            var now = DateTime.UtcNow;
            var id = 1L;
            var rawSql = @"INSERT INTO `TableWithAllColumnTypes` 
                              (`ColBool`, `ColBoolArray`, `ColBytes`, `ColBytesMax`, `ColBytesArray`, `ColBytesMaxArray`,
                               `ColDate`, `ColDateArray`, `ColFloat64`, `ColFloat64Array`, `ColInt64`, `ColInt64Array`,
                               `ColNumeric`, `ColNumericArray`, `ColString`, `ColStringArray`, `ColStringMax`, `ColStringMaxArray`,
                               `ColTimestamp`, `ColTimestampArray`, `ColJson`, `ColJsonArray`)
                              VALUES
                              (:ColBool, :ColBoolArray, :ColBytes, :ColBytesMax, :ColBytesArray, :ColBytesMaxArray,
                               :ColDate, :ColDateArray, :ColFloat64, :ColFloat64Array, :ColInt64, :ColInt64Array,
                               :ColNumeric, :ColNumericArray, :ColString, :ColStringArray, :ColStringMax, :ColStringMaxArray,
                               :ColTimestamp, :ColTimestampArray, :ColJson, :ColJsonArray)";
            var translatedSql = @"INSERT INTO `TableWithAllColumnTypes` 
                              (`ColBool`, `ColBoolArray`, `ColBytes`, `ColBytesMax`, `ColBytesArray`, `ColBytesMaxArray`,
                               `ColDate`, `ColDateArray`, `ColFloat64`, `ColFloat64Array`, `ColInt64`, `ColInt64Array`,
                               `ColNumeric`, `ColNumericArray`, `ColString`, `ColStringArray`, `ColStringMax`, `ColStringMaxArray`,
                               `ColTimestamp`, `ColTimestampArray`, `ColJson`, `ColJsonArray`)
                              VALUES
                              (@p0, @p1, @p2, @p3, @p4, @p5,
                               @p6, @p7, @p8, @p9, @p10, @p11,
                               @p12, @p13, @p14, @p15, @p16, @p17,
                               @p18, @p19, @p20, @p21)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(translatedSql, StatementResult.CreateUpdateCount(1L));

            var row = new TableWithAllColumnTypes
            {
                ColBool = true,
                ColBoolArray = new SpannerBoolArray(new List<bool?> { true, false, true }),
                ColBytes = new byte[] { 1, 2, 3 },
                ColBytesMax = Encoding.UTF8.GetBytes("This is a long string"),
                ColBytesArray = new SpannerBytesArray(new List<byte[]> { new byte[] { 3, 2, 1 }, new byte[] { }, new byte[] { 4, 5, 6 } }),
                ColBytesMaxArray = new SpannerBytesArray(new List<byte[]> { Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2"), Encoding.UTF8.GetBytes("string 3") }),
                ColDate = new SpannerDate(2020, 12, 28),
                ColDateArray = new SpannerDateArray(new List<DateTime?> { new DateTime(2020, 12, 28), new DateTime(2010, 1, 1), today.ToDateTime() }),
                ColFloat64 = 3.14D,
                ColFloat64Array = new SpannerFloat64Array(new List<double?> { 3.14D, 6.626D }),
                ColInt64 = id,
                ColInt64Array = new SpannerInt64Array(new List<long?> { 1L, 2L, 4L, 8L }),
                ColNumeric = new SpannerNumeric((V1.SpannerNumeric)3.14m),
                ColNumericArray = new SpannerNumericArray(new List<V1.SpannerNumeric?> { (V1.SpannerNumeric)3.14m, (V1.SpannerNumeric)6.626m }),
                ColString = "some string",
                ColStringArray = new SpannerStringArray(new List<string> { "string1", "string2", "string3" }),
                ColStringMax = "some longer string",
                ColStringMaxArray = new SpannerStringArray(new List<string> { "longer string1", "longer string2", "longer string3" }),
                ColTimestamp = new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288),
                ColTimestampArray = new SpannerTimestampArray(new List<DateTime?> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now }),
                ColJson = new SpannerJson("{\"key1\": \"value1\", \"key2\": \"value2\"}"),
                ColJsonArray = new SpannerJsonArray(new List<string>{ "{\"key1\": \"value1\", \"key2\": \"value2\"}", "{\"key1\": \"value3\", \"key2\": \"value4\"}" }),
            };
            var statement = session.CreateSQLQuery(rawSql);
            statement.SetParameter("ColBool", row.ColBool);
            statement.SetParameter("ColBoolArray", row.ColBoolArray);
            statement.SetParameter("ColBytes", row.ColBytes);
            statement.SetParameter("ColBytesMax", row.ColBytesMax);
            statement.SetParameter("ColBytesArray", row.ColBytesArray);
            statement.SetParameter("ColBytesMaxArray", row.ColBytesMaxArray);
            statement.SetParameter("ColDate", row.ColDate);
            statement.SetParameter("ColDateArray", row.ColDateArray);
            statement.SetParameter("ColFloat64", row.ColFloat64);
            statement.SetParameter("ColFloat64Array", row.ColFloat64Array);
            statement.SetParameter("ColInt64", row.ColInt64);
            statement.SetParameter("ColInt64Array", row.ColInt64Array);
            statement.SetParameter("ColNumeric", row.ColNumeric);
            statement.SetParameter("ColNumericArray", row.ColNumericArray);
            statement.SetParameter("ColString", row.ColString);
            statement.SetParameter("ColStringArray", row.ColStringArray);
            statement.SetParameter("ColStringMax", row.ColStringMax);
            statement.SetParameter("ColStringMaxArray", row.ColStringMaxArray);
            statement.SetParameter("ColTimestamp", row.ColTimestamp.Value);
            statement.SetParameter("ColTimestampArray", row.ColTimestampArray);
            statement.SetParameter("ColJson", row.ColJson);
            statement.SetParameter("ColJsonArray", row.ColJsonArray);

            var updateCount = await statement.ExecuteUpdateAsync();

            Assert.Equal(1, updateCount);
            // Verify that the INSERT statement is the only one on the mock server.
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(r => r is V1.ExecuteSqlRequest sqlRequest).Select(r => r as V1.ExecuteSqlRequest),
                request =>
                {
                    Assert.Equal(translatedSql, request.Sql);
                    Assert.Collection(request.ParamTypes,
                        paramType =>
                        {
                            Assert.Equal("p0", paramType.Key);
                            Assert.Equal(V1.TypeCode.Bool, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p1", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Bool, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p2", paramType.Key);
                            Assert.Equal(V1.TypeCode.Bytes, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p3", paramType.Key);
                            Assert.Equal(V1.TypeCode.Bytes, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p4", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Bytes, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p5", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Bytes, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p6", paramType.Key);
                            Assert.Equal(V1.TypeCode.Date, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p7", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Date, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p8", paramType.Key);
                            Assert.Equal(V1.TypeCode.Float64, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p9", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Float64, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p10", paramType.Key);
                            Assert.Equal(V1.TypeCode.Int64, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p11", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Int64, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p12", paramType.Key);
                            Assert.Equal(V1.TypeCode.Numeric, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p13", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Numeric, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p14", paramType.Key);
                            Assert.Equal(V1.TypeCode.String, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p15", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.String, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p16", paramType.Key);
                            Assert.Equal(V1.TypeCode.String, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p17", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.String, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p18", paramType.Key);
                            Assert.Equal(V1.TypeCode.Timestamp, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p19", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Timestamp, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p20", paramType.Key);
                            Assert.Equal(V1.TypeCode.Json, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p21", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Json, paramType.Value.ArrayElementType.Code);
                        }
                    );
                });
        }

        [Fact]
        public async Task CanInsertRecordWithAllTypes()
        {
            var insertSql = "INSERT INTO TableWithAllColumnTypes (ColFloat64, ColNumeric, ColBool, ColString, ColStringMax, ColBytes, ColBytesMax, ColDate, ColTimestamp, ColJson, ColInt64Array, ColFloat64Array, ColNumericArray, ColBoolArray, ColStringArray, ColStringMaxArray, ColBytesArray, ColBytesMaxArray, ColDateArray, ColTimestampArray, ColJsonArray, ASC, ColCommitTs, ColInt64) VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20, @p21, PENDING_COMMIT_TIMESTAMP(), @p22)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            var selectSql =
                "SELECT tablewitha_.ColComputed as colcomputed24_2_ FROM TableWithAllColumnTypes tablewitha_ WHERE tablewitha_.ColInt64=@p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectSql, StatementResult.CreateResultSet(
                new[]{ new Tuple<V1.TypeCode, string>(V1.TypeCode.String, "colcomputed24_2_")},
                new []{new []{"Test"}}));
            
            using var session = _fixture.SessionFactory.OpenSession();
            var row = CreateRowWithAllColumnTypes();
            await session.SaveAsync(row);
            await session.FlushAsync();
            VerifyTableWithAllColumnTypesParameters(row, insertSql);
        }

        [Fact]
        public async Task CanUpdateRecordWithAllTypes()
        {
            var updateSql =
                "UPDATE TableWithAllColumnTypes SET ColFloat64 = @p0, ColNumeric = @p1, ColBool = @p2, ColString = @p3, ColStringMax = @p4, ColBytes = @p5, ColBytesMax = @p6, ColDate = @p7, ColTimestamp = @p8, ColJson = @p9, ColInt64Array = @p10, ColFloat64Array = @p11, ColNumericArray = @p12, ColBoolArray = @p13, ColStringArray = @p14, ColStringMaxArray = @p15, ColBytesArray = @p16, ColBytesMaxArray = @p17, ColDateArray = @p18, ColTimestampArray = @p19, ColJsonArray = @p20, ASC = @p21 WHERE ColInt64 = @p22";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));
            var selectSql =
                "SELECT tablewitha_.ColComputed as colcomputed24_2_ FROM TableWithAllColumnTypes tablewitha_ WHERE tablewitha_.ColInt64=@p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectSql, StatementResult.CreateResultSet(
                new[] { new Tuple<V1.TypeCode, string>(V1.TypeCode.String, "colcomputed24_2_") },
                new[] { new[] { "Test" } }));

            using var session = _fixture.SessionFactory.OpenSession();
            var row = CreateRowWithAllColumnTypes();
            await session.UpdateAsync(row);
            await session.FlushAsync();
            VerifyTableWithAllColumnTypesParameters(row, updateSql);
        }
        
        private TableWithAllColumnTypes CreateRowWithAllColumnTypes() => 
            new TableWithAllColumnTypes
            {
                ColBool = true,
                ColBoolArray = new SpannerBoolArray(new List<bool?> { true, false, true, null }),
                ColBytes = new byte[] { 1, 2, 3 },
                ColBytesMax = Encoding.UTF8.GetBytes("This is a long string"),
                ColBytesArray = new SpannerBytesArray(new List<byte[]>
                    { new byte[] { 3, 2, 1 }, new byte[] { }, new byte[] { 4, 5, 6 }, null }),
                ColBytesMaxArray = new SpannerBytesArray(new List<byte[]>
                {
                    Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2"),
                    Encoding.UTF8.GetBytes("string 3"), null
                }),
                ColDate = new SpannerDate(2020, 12, 28),
                ColDateArray = new SpannerDateArray(new List<DateTime?>
                    { new DateTime(2020, 12, 28), new DateTime(2010, 1, 1), null }),
                ColFloat64 = 3.14D,
                ColFloat64Array = new SpannerFloat64Array(new List<double?> { 3.14D, 6.626D, null }),
                ColInt64 = 1L,
                ColInt64Array = new SpannerInt64Array(new List<long?> { 1L, 2L, 4L, 8L, null }),
                ColNumeric = new SpannerNumeric((V1.SpannerNumeric)3.14m),
                ColNumericArray = new SpannerNumericArray(new List<V1.SpannerNumeric?>
                    { (V1.SpannerNumeric)3.14m, (V1.SpannerNumeric)6.626m, null }),
                ColString = "some string",
                ColStringArray = new SpannerStringArray(new List<string> { "string1", "string2", "string3", null }),
                ColStringMax = "some longer string",
                ColStringMaxArray = new SpannerStringArray(new List<string>
                    { "longer string1", "longer string2", "longer string3", null }),
                ColTimestamp = new DateTime(2020, 12, 28, 15, 16, 28, 148, DateTimeKind.Utc).AddTicks(1839288),
                ColTimestampArray = new SpannerTimestampArray(new List<DateTime?>
                    { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), null }),
                ColJson = new SpannerJson("{\"key1\": \"value1\", \"key2\": \"value2\"}"),
                ColJsonArray = new SpannerJsonArray(new List<string>
                {
                    "{\"key1\": \"value1\", \"key2\": \"value2\"}", "{\"key1\": \"value3\", \"key2\": \"value4\"}", null
                }),
            };

        private void VerifyTableWithAllColumnTypesParameters(TableWithAllColumnTypes row, string sql)
        {
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(r => r is V1.ExecuteSqlRequest sqlRequest && sqlRequest.Sql == sql).Select(r => r as V1.ExecuteSqlRequest),
                request =>
                {
                    Assert.Collection(request.ParamTypes,
                        paramType =>
                        {
                            Assert.Equal("p0", paramType.Key);
                            Assert.Equal(V1.TypeCode.Float64, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p1", paramType.Key);
                            Assert.Equal(V1.TypeCode.Numeric, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p2", paramType.Key);
                            Assert.Equal(V1.TypeCode.Bool, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p3", paramType.Key);
                            Assert.Equal(V1.TypeCode.String, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p4", paramType.Key);
                            Assert.Equal(V1.TypeCode.String, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p5", paramType.Key);
                            Assert.Equal(V1.TypeCode.Bytes, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p6", paramType.Key);
                            Assert.Equal(V1.TypeCode.Bytes, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p7", paramType.Key);
                            Assert.Equal(V1.TypeCode.Date, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p8", paramType.Key);
                            Assert.Equal(V1.TypeCode.Timestamp, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p9", paramType.Key);
                            Assert.Equal(V1.TypeCode.Json, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p10", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Int64, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p11", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Float64, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p12", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Numeric, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p13", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Bool, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p14", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.String, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p15", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.String, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p16", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Bytes, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p17", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Bytes, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p18", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Date, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p19", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Timestamp, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            Assert.Equal("p20", paramType.Key);
                            Assert.Equal(V1.TypeCode.Array, paramType.Value.Code);
                            Assert.Equal(V1.TypeCode.Json, paramType.Value.ArrayElementType.Code);
                        },
                        paramType =>
                        {
                            // ASC
                            Assert.Equal("p21", paramType.Key);
                            Assert.Equal(V1.TypeCode.String, paramType.Value.Code);
                        },
                        paramType =>
                        {
                            // ColInt64
                            Assert.Equal("p22", paramType.Key);
                            Assert.Equal(V1.TypeCode.Int64, paramType.Value.Code);
                        }
                    );
                });
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(r => r is V1.ExecuteSqlRequest sqlRequest && sqlRequest.Sql == sql).Select(r => r as V1.ExecuteSqlRequest),
                request =>
                {
                    Assert.Collection(request.Params.Fields,
                        param =>
                        {
                            Assert.Equal("p0", param.Key);
                            Assert.Equal(row.ColFloat64, param.Value.NumberValue);
                        },
                        param =>
                        {
                            Assert.Equal("p1", param.Key);
                            Assert.Equal(row.ColNumeric.ToString(), param.Value.StringValue);
                        },
                        param =>
                        {
                            Assert.Equal("p2", param.Key);
                            Assert.Equal(row.ColBool, param.Value.BoolValue);
                        },
                        param =>
                        {
                            Assert.Equal("p3", param.Key);
                            Assert.Equal(row.ColString, param.Value.StringValue);
                        },
                        param =>
                        {
                            Assert.Equal("p4", param.Key);
                            Assert.Equal(row.ColStringMax, param.Value.StringValue);
                        },
                        param =>
                        {
                            Assert.Equal("p5", param.Key);
                            Assert.Equal(Convert.ToBase64String(row.ColBytes), param.Value.StringValue);
                        },
                        param =>
                        {
                            Assert.Equal("p6", param.Key);
                            Assert.Equal(Convert.ToBase64String(row.ColBytesMax), param.Value.StringValue);
                        },
                        param =>
                        {
                            Assert.Equal("p7", param.Key);
                            Assert.Equal(row.ColDate.ToString(), param.Value.StringValue);
                        },
                        param =>
                        {
                            Assert.Equal("p8", param.Key);
                            Assert.Equal(row.ColTimestamp!.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ"), param.Value.StringValue);
                        },
                        param =>
                        {
                            Assert.Equal("p9", param.Key);
                            Assert.Equal(row.ColJson.Json, param.Value.StringValue);
                        },
                        param =>
                        {
                            Assert.Equal("p10", param.Key);
                            Assert.Collection(param.Value.ListValue.Values,
                                value => Assert.Equal(row.ColInt64Array.Array[0]!.Value.ToString(), value.StringValue),
                                value => Assert.Equal(row.ColInt64Array.Array[1]!.Value.ToString(), value.StringValue),
                                value => Assert.Equal(row.ColInt64Array.Array[2]!.Value.ToString(), value.StringValue),
                                value => Assert.Equal(row.ColInt64Array.Array[3]!.Value.ToString(), value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase)
                            );
                        },
                        param =>
                        {
                            Assert.Equal("p11", param.Key);
                            Assert.Collection(param.Value.ListValue.Values,
                                value => Assert.Equal(row.ColFloat64Array.Array[0]!.Value, value.NumberValue),
                                value => Assert.Equal(row.ColFloat64Array.Array[1]!.Value, value.NumberValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase)
                            );
                        },
                        param =>
                        {
                            Assert.Equal("p12", param.Key);
                            Assert.Collection(param.Value.ListValue.Values,
                                value => Assert.Equal(row.ColNumericArray.Array[0]!.Value.ToString(), value.StringValue),
                                value => Assert.Equal(row.ColNumericArray.Array[1]!.Value.ToString(), value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase)
                            );
                        },
                        param =>
                        {
                            Assert.Equal("p13", param.Key);
                            Assert.Collection(param.Value.ListValue.Values,
                                value => Assert.Equal(row.ColBoolArray.Array[0]!.Value, value.BoolValue),
                                value => Assert.Equal(row.ColBoolArray.Array[1]!.Value, value.BoolValue),
                                value => Assert.Equal(row.ColBoolArray.Array[2]!.Value, value.BoolValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase)
                            );
                        },
                        param =>
                        {
                            Assert.Equal("p14", param.Key);
                            Assert.Collection(param.Value.ListValue.Values,
                                value => Assert.Equal(row.ColStringArray.Array[0], value.StringValue),
                                value => Assert.Equal(row.ColStringArray.Array[1], value.StringValue),
                                value => Assert.Equal(row.ColStringArray.Array[2], value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase)
                            );
                        },
                        param =>
                        {
                            Assert.Equal("p15", param.Key);
                            Assert.Collection(param.Value.ListValue.Values,
                                value => Assert.Equal(row.ColStringMaxArray.Array[0], value.StringValue),
                                value => Assert.Equal(row.ColStringMaxArray.Array[1], value.StringValue),
                                value => Assert.Equal(row.ColStringMaxArray.Array[2], value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase)
                            );
                        },
                        param =>
                        {
                            Assert.Equal("p16", param.Key);
                            Assert.Collection(param.Value.ListValue.Values,
                                value => Assert.Equal(Convert.ToBase64String(row.ColBytesArray.Array[0]), value.StringValue),
                                value => Assert.Equal(Convert.ToBase64String(row.ColBytesArray.Array[1]), value.StringValue),
                                value => Assert.Equal(Convert.ToBase64String(row.ColBytesArray.Array[2]), value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase)
                            );
                        },
                        param =>
                        {
                            Assert.Equal("p17", param.Key);
                            Assert.Collection(param.Value.ListValue.Values,
                                value => Assert.Equal(Convert.ToBase64String(row.ColBytesMaxArray.Array[0]), value.StringValue),
                                value => Assert.Equal(Convert.ToBase64String(row.ColBytesMaxArray.Array[1]), value.StringValue),
                                value => Assert.Equal(Convert.ToBase64String(row.ColBytesMaxArray.Array[2]), value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase)
                            );
                        },
                        param =>
                        {
                            Assert.Equal("p18", param.Key);
                            Assert.Collection(param.Value.ListValue.Values,
                                value => Assert.Equal(SpannerDate.FromDateTime(row.ColDateArray.Array[0]!.Value).ToString(), value.StringValue),
                                value => Assert.Equal(SpannerDate.FromDateTime(row.ColDateArray.Array[1]!.Value).ToString(), value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase)
                            );
                        },
                        param =>
                        {
                            Assert.Equal("p19", param.Key);
                            Assert.Collection(param.Value.ListValue.Values,
                                value => Assert.Equal(row.ColTimestampArray.Array[0]!.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ"), value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase)
                            );
                        },
                        param =>
                        {
                            Assert.Equal("p20", param.Key);
                            Assert.Collection(param.Value.ListValue.Values,
                                value => Assert.Equal(row.ColJsonArray.Array[0], value.StringValue),
                                value => Assert.Equal(row.ColJsonArray.Array[1], value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase)
                            );
                        },
                        param =>
                        {
                            // ASC
                            Assert.Equal("p21", param.Key);
                            Assert.Equal(Value.KindOneofCase.NullValue, param.Value.KindCase);
                        },
                        paramType =>
                        {
                            // ColInt64
                            Assert.Equal("p22", paramType.Key);
                            Assert.Equal(row.ColInt64.ToString(), paramType.Value.StringValue);
                        }
                    );
                });
        }

        [Fact]
        public async Task CanInsertRecordWithAllTypesWithNullValues()
        {
            var insertSql = "INSERT INTO TableWithAllColumnTypes (ColFloat64, ColNumeric, ColBool, ColString, ColStringMax, ColBytes, ColBytesMax, ColDate, ColTimestamp, ColJson, ColInt64Array, ColFloat64Array, ColNumericArray, ColBoolArray, ColStringArray, ColStringMaxArray, ColBytesArray, ColBytesMaxArray, ColDateArray, ColTimestampArray, ColJsonArray, ASC, ColCommitTs, ColInt64) VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20, @p21, PENDING_COMMIT_TIMESTAMP(), @p22)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            var selectSql =
                "SELECT tablewitha_.ColComputed as colcomputed24_2_ FROM TableWithAllColumnTypes tablewitha_ WHERE tablewitha_.ColInt64=@p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectSql, StatementResult.CreateResultSet(
                new[]{ new Tuple<V1.TypeCode, string>(V1.TypeCode.String, "colcomputed24_2_")},
                new []{new []{"test"}}));
            
            using var session = _fixture.SessionFactory.OpenSession();
            var row = new TableWithAllColumnTypes { ColInt64 = 1L };
            await session.SaveAsync(row);
            await session.FlushAsync();

            var request = _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>().First(req => req.Sql == insertSql);
            foreach (var param in request.Params.Fields)
            {
                // Only the id should be filled.
                if (param.Key == "p22")
                {
                    Assert.Equal("1", param.Value.StringValue);
                }
                else
                {
                    Assert.Equal(Value.KindOneofCase.NullValue, param.Value.KindCase);
                }
            }
        }

        [Fact]
        public async Task CanSelectTableWithAllColumnTypes()
        {
            var sql = "SELECT tablewitha0_.ColInt64 as colint1_2_0_, tablewitha0_.ColFloat64 as colfloat2_2_0_, tablewitha0_.ColNumeric as colnumeric3_2_0_, tablewitha0_.ColBool as colbool4_2_0_, tablewitha0_.ColString as colstring5_2_0_, tablewitha0_.ColStringMax as colstringmax6_2_0_, tablewitha0_.ColBytes as colbytes7_2_0_, tablewitha0_.ColBytesMax as colbytesmax8_2_0_, tablewitha0_.ColDate as coldate9_2_0_, tablewitha0_.ColTimestamp as coltimestamp10_2_0_, tablewitha0_.ColJson as coljson11_2_0_, tablewitha0_.ColCommitTs as colcommitts12_2_0_, tablewitha0_.ColInt64Array as colint13_2_0_, tablewitha0_.ColFloat64Array as colfloat14_2_0_, tablewitha0_.ColNumericArray as colnumericarray15_2_0_, tablewitha0_.ColBoolArray as colboolarray16_2_0_, tablewitha0_.ColStringArray as colstringarray17_2_0_, tablewitha0_.ColStringMaxArray as colstringmaxarray18_2_0_, tablewitha0_.ColBytesArray as colbytesarray19_2_0_, tablewitha0_.ColBytesMaxArray as colbytesmaxarray20_2_0_, tablewitha0_.ColDateArray as coldatearray21_2_0_, tablewitha0_.ColTimestampArray as coltimestamparray22_2_0_, tablewitha0_.ColJsonArray as coljsonarray23_2_0_, tablewitha0_.ColComputed as colcomputed24_2_0_, tablewitha0_.ASC as asc25_2_0_ FROM TableWithAllColumnTypes tablewitha0_ WHERE tablewitha0_.ColInt64=@p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, CreateTableWithAllColumnTypesResultSet(CreateRowWithAllColumnTypes()));
            using var session = _fixture.SessionFactory.OpenSession();
            var row = await session.GetAsync<TableWithAllColumnTypes>(1L);
            var compare = CreateRowWithAllColumnTypes();
            Assert.Equal(compare, row);
        }

        [Fact]
        public async Task RequestIncludesEfCoreClientHeader()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            AddSingerResult(GetSelectSingerSql());
            await session.GetAsync<Singer>(1L);

            Assert.NotEmpty(_fixture.SpannerMock.Contexts);
            Assert.All(_fixture.SpannerMock.Contexts, context =>
            {
                var entry = context.RequestHeaders.Get("x-goog-api-client");
                Assert.NotNull(entry);
                Assert.Contains("nhibernate", entry.Value);
            });
        }

        [Fact]
        public async Task CanUseStatementHint()
        {
            var sql =
                "@{OPTIMIZER_VERSION=1} select singer0_.SingerId as singerid1_0_, singer0_.FirstName as firstname2_0_, singer0_.LastName as lastname3_0_, singer0_.FullName as fullname4_0_, singer0_.BirthDate as birthdate5_0_, singer0_.Picture as picture6_0_ from Singer singer0_ where singer0_.LastName=@p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .Query<Singer>()
                .SetStatementHint("@{OPTIMIZER_VERSION=1}")
                .Where(s => s.LastName.Equals("Peterson"))
                .ToListAsync();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        [Fact]
        public async Task CanUseTableHint()
        {
            var sql =
                "select singer0_.SingerId as singerid1_0_, singer0_.FirstName as firstname2_0_, singer0_.LastName as lastname3_0_, singer0_.FullName as fullname4_0_, singer0_.BirthDate as birthdate5_0_, singer0_.Picture as picture6_0_ from Singer@{FORCE_INDEX=Idx_Singers_FullName}  singer0_ where singer0_.LastName=@p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .Query<Singer>()
                .SetTableHint("Singer", "@{FORCE_INDEX=Idx_Singers_FullName}")
                .Where(s => s.LastName.Equals("Peterson"))
                .ToListAsync();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        [Fact]
        public async Task CanUseTableHintOnJoin()
        {
            var sql =
                "select singer1_.SingerId as singerid1_0_, singer1_.FirstName as firstname2_0_, singer1_.LastName as lastname3_0_, singer1_.FullName as fullname4_0_, singer1_.BirthDate as birthdate5_0_, singer1_.Picture as picture6_0_ from Album@{FORCE_INDEX=Idx_Albums_Title} album0_ left outer join Singer@{FORCE_INDEX=Idx_Singers_FullName} singer1_ on album0_.SingerId=singer1_.SingerId where singer1_.LastName=@p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .Query<Album>()
                .Select(a => a.Singer)
                .SetTableHints(new Dictionary<string, string>
                {
                    {"Singer", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Album", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .Where(s => s.LastName.Equals("Peterson"))
                .ToListAsync();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        [Fact]
        public async Task CanUseStatementAndTableHints()
        {
            var sql =
                "@{OPTIMIZER_VERSION=1}select singer1_.SingerId as singerid1_0_, singer1_.FirstName as firstname2_0_, singer1_.LastName as lastname3_0_, singer1_.FullName as fullname4_0_, singer1_.BirthDate as birthdate5_0_, singer1_.Picture as picture6_0_ from Album@{FORCE_INDEX=Idx_Albums_Title} album0_ left outer join Singer@{FORCE_INDEX=Idx_Singers_FullName} singer1_ on album0_.SingerId=singer1_.SingerId where singer1_.LastName=@p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .Query<Album>()
                .Select(a => a.Singer)
                .SetStatementAndTableHints("@{OPTIMIZER_VERSION=1}", new Dictionary<string, string>
                {
                    {"Singer", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Album", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .Where(s => s.LastName.Equals("Peterson"))
                .ToListAsync();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        [Fact]
        public async Task CanUseStatementHintWithHql()
        {
            var sql =
                "@{OPTIMIZER_VERSION=1} select singer0_.SingerId as singerid1_0_, singer0_.FirstName as firstname2_0_, singer0_.LastName as lastname3_0_, singer0_.FullName as fullname4_0_, singer0_.BirthDate as birthdate5_0_, singer0_.Picture as picture6_0_ from Singer singer0_ where singer0_.LastName=@p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .CreateQuery("from Singer where LastName = :lastName")
                .SetStatementHint("@{OPTIMIZER_VERSION=1}")
                .SetParameter("lastName", "Peterson")
                .ListAsync<Singer>();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        [Fact]
        public async Task CanUseTableHintWithHql()
        {
            var sql =
                "select singer0_.SingerId as singerid1_0_, singer0_.FirstName as firstname2_0_, singer0_.LastName as lastname3_0_, singer0_.FullName as fullname4_0_, singer0_.BirthDate as birthdate5_0_, singer0_.Picture as picture6_0_ from Singer@{FORCE_INDEX=Idx_Singers_FullName}  singer0_ where singer0_.LastName=@p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .CreateQuery("from Singer where LastName = :lastName")
                .SetTableHint("Singer", "@{FORCE_INDEX=Idx_Singers_FullName}")
                .SetParameter("lastName", "Peterson")
                .ListAsync<Singer>();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        [Fact]
        public async Task CanUseTableHintOnJoinWithHql()
        {
            var sql =
                "select album0_.AlbumId as albumid1_1_0_, singer1_.SingerId as singerid1_0_1_, album0_.Title as title2_1_0_, album0_.ReleaseDate as releasedate3_1_0_, album0_.SingerId as singerid4_1_0_, singer1_.FirstName as firstname2_0_1_, singer1_.LastName as lastname3_0_1_, singer1_.FullName as fullname4_0_1_, singer1_.BirthDate as birthdate5_0_1_, singer1_.Picture as picture6_0_1_ "
                    + "from Album@{FORCE_INDEX=Idx_Albums_Title} album0_ "
                    + "left outer join Singer@{FORCE_INDEX=Idx_Singers_FullName} singer1_ on album0_.SingerId=singer1_.SingerId "
                    + "where singer1_.LastName=@p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .CreateQuery("from Album as album left outer join album.Singer as singer where singer.LastName = :lastName")
                .SetTableHints(new Dictionary<string, string>
                {
                    {"Singer", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Album", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .SetParameter("lastName", "Peterson")
                .ListAsync<Album>();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        [Fact]
        public async Task CanUseStatementAndTableHintsWithHql()
        {
            var sql =
                "@{OPTIMIZER_VERSION=1}select album0_.AlbumId as albumid1_1_0_, singer1_.SingerId as singerid1_0_1_, album0_.Title as title2_1_0_, album0_.ReleaseDate as releasedate3_1_0_, album0_.SingerId as singerid4_1_0_, singer1_.FirstName as firstname2_0_1_, singer1_.LastName as lastname3_0_1_, singer1_.FullName as fullname4_0_1_, singer1_.BirthDate as birthdate5_0_1_, singer1_.Picture as picture6_0_1_ "
                + "from Album@{FORCE_INDEX=Idx_Albums_Title} album0_ "
                + "left outer join Singer@{FORCE_INDEX=Idx_Singers_FullName} singer1_ on album0_.SingerId=singer1_.SingerId "
                + "where singer1_.LastName=@p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .CreateQuery("from Album as album left outer join album.Singer as singer where singer.LastName = :lastName")
                .SetStatementAndTableHints("@{OPTIMIZER_VERSION=1}", new Dictionary<string, string>
                {
                    {"Singer", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Album", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .SetParameter("lastName", "Peterson")
                .ListAsync<Album>();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        [Fact]
        public async Task CanUseStatementHintWithCriteria()
        {
            var sql =
                "@{OPTIMIZER_VERSION=1} /* criteria query */ SELECT this_.SingerId as singerid1_0_0_, this_.FirstName as firstname2_0_0_, this_.LastName as lastname3_0_0_, this_.FullName as fullname4_0_0_, this_.BirthDate as birthdate5_0_0_, this_.Picture as picture6_0_0_ FROM Singer this_ WHERE this_.LastName = @p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .CreateCriteria(typeof(Singer))
                .Add(Restrictions.Eq("LastName", "Peterson"))
                .SetStatementHint("@{OPTIMIZER_VERSION=1}")
                .ListAsync<Singer>();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        [Fact]
        public async Task CanUseTableHintWithCriteria()
        {
            var sql =
                "/* criteria query */ SELECT this_.SingerId as singerid1_0_0_, this_.FirstName as firstname2_0_0_, this_.LastName as lastname3_0_0_, this_.FullName as fullname4_0_0_, this_.BirthDate as birthdate5_0_0_, this_.Picture as picture6_0_0_ "
                    + "FROM Singer@{FORCE_INDEX=Idx_Singers_FullName}  this_ WHERE this_.LastName = @p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .CreateCriteria(typeof(Singer))
                .Add(Restrictions.Eq("LastName", "Peterson"))
                .SetTableHint("Singer", "@{FORCE_INDEX=Idx_Singers_FullName}")
                .ListAsync<Singer>();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        [Fact]
        public async Task CanUseTableHintOnJoinWithCriteria()
        {
            var sql =
                "/* criteria query */ SELECT this_.AlbumId as albumid1_1_1_, this_.Title as title2_1_1_, this_.ReleaseDate as releasedate3_1_1_, this_.SingerId as singerid4_1_1_, singer1_.SingerId as singerid1_0_0_, singer1_.FirstName as firstname2_0_0_, singer1_.LastName as lastname3_0_0_, singer1_.FullName as fullname4_0_0_, singer1_.BirthDate as birthdate5_0_0_, singer1_.Picture as picture6_0_0_ "
                    + "FROM Album@{FORCE_INDEX=Idx_Albums_Title} this_ "
                    + "left outer join Singer@{FORCE_INDEX=Idx_Singers_FullName} singer1_ on this_.SingerId=singer1_.SingerId "
                    + "WHERE singer1_.LastName = @p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .CreateCriteria(typeof(Album))
                .CreateAlias("Singer", "singer", JoinType.LeftOuterJoin)
                .Add(Restrictions.Eq("singer.LastName", "Peterson"))
                .SetTableHints(new Dictionary<string, string>
                {
                    {"Singer", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Album", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .ListAsync<Album>();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        [Fact]
        public async Task CanUseStatementAndTableHintsWithCriteria()
        {
            var sql =
                "@{OPTIMIZER_VERSION=1}/* criteria query */ SELECT this_.AlbumId as albumid1_1_1_, this_.Title as title2_1_1_, this_.ReleaseDate as releasedate3_1_1_, this_.SingerId as singerid4_1_1_, singer1_.SingerId as singerid1_0_0_, singer1_.FirstName as firstname2_0_0_, singer1_.LastName as lastname3_0_0_, singer1_.FullName as fullname4_0_0_, singer1_.BirthDate as birthdate5_0_0_, singer1_.Picture as picture6_0_0_ "
                    + "FROM Album@{FORCE_INDEX=Idx_Albums_Title} this_ "
                    + "left outer join Singer@{FORCE_INDEX=Idx_Singers_FullName} singer1_ on this_.SingerId=singer1_.SingerId "
                    + "WHERE singer1_.LastName = @p0";
            AddEmptySingerResult(sql);
            using var session = _fixture.SessionFactoryWithComments.OpenSession();
            await session
                .CreateCriteria(typeof(Album))
                .CreateAlias("Singer", "singer", JoinType.LeftOuterJoin)
                .Add(Restrictions.Eq("singer.LastName", "Peterson"))
                .SetStatementAndTableHints("@{OPTIMIZER_VERSION=1}", new Dictionary<string, string>
                {
                    {"Singer", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Album", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .ListAsync<Album>();
            
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal(sql, request.Sql));
        }

        private static string GetSelectSingerSql() =>
            "SELECT singer0_.SingerId as singerid1_0_0_, singer0_.FirstName as firstname2_0_0_, singer0_.LastName as lastname3_0_0_, "
            + "singer0_.FullName as fullname4_0_0_, singer0_.BirthDate as birthdate5_0_0_, singer0_.Picture as picture6_0_0_ "
            + "FROM Singer singer0_ WHERE singer0_.SingerId=@p0";
        
        private void AddEmptySingerResult(string sql) => AddSingerResults(sql, new List<object[]>());

        private string AddSingerResult(string sql) =>
            AddSingerResults(sql, new List<object[]>
            {
                new object[] { 1L, "Alice", "Morrison", "Alice Morrison", "1998-05-12", Convert.ToBase64String(new byte[]{1,2,3}) },
            });

        private string AddSingerResults(string sql, IEnumerable<object[]> rows)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "singerid1_0_0_"),
                    Tuple.Create(V1.TypeCode.String, "firstname2_0_0_"),
                    Tuple.Create(V1.TypeCode.String, "lastname3_0_0_"),
                    Tuple.Create(V1.TypeCode.String, "fullname4_0_0_"),
                    Tuple.Create(V1.TypeCode.Date, "birthdate5_0_0_"),
                    Tuple.Create(V1.TypeCode.Bytes, "picture6_0_0_"),
                }, rows));
            return sql;
        }

        private string AddSelectSingerFullNameResult(string fullName, int paramIndex)
        {
            var selectFullNameSql =
                $"SELECT singer_.FullName as fullname4_0_ FROM Singer singer_ WHERE singer_.SingerId=@p{paramIndex}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectFullNameSql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "fullname4_0_"),
                },
                new List<object[]>
                {
                    new object[] { fullName },
                }
            ));
            return selectFullNameSql;
        }
        
        private static string GetSelectSingerAlbumsSql() =>
            "SELECT albums0_.Singer as singer5_1_1_, albums0_.AlbumId as albumid1_1_1_, "
            + "albums0_.AlbumId as albumid1_1_0_, albums0_.Title as title2_1_0_, "
            + "albums0_.ReleaseDate as releasedate3_1_0_, albums0_.SingerId as singerid4_1_0_ "
            + "FROM Album albums0_ WHERE albums0_.Singer=@p0";
        
        private string AddSingerAlbumsResults(string sql, IEnumerable<object[]> rows)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "singer5_1_1_"),
                    Tuple.Create(V1.TypeCode.Int64, "albumid1_1_1_"),
                    Tuple.Create(V1.TypeCode.Int64, "albumid1_1_0_"),
                    Tuple.Create(V1.TypeCode.String, "title2_1_0_"),
                    Tuple.Create(V1.TypeCode.Date, "releasedate3_1_0_"),
                    Tuple.Create(V1.TypeCode.Int64, "singerid4_1_0_"),
                }, rows));
            return sql;
        }

        private StatementResult CreateTableWithAllColumnTypesResultSet(TableWithAllColumnTypes row)
        {
            return StatementResult.CreateResultSet(new List<Tuple<V1.Type, string>>
            {
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Int64}, "colint1_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Float64}, "colfloat2_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Numeric}, "colnumeric3_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Bool}, "colbool4_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.String}, "colstring5_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.String}, "colstringmax6_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Bytes}, "colbytes7_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Bytes}, "colbytesmax8_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Date}, "coldate9_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Timestamp}, "coltimestamp10_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Json}, "coljson11_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Timestamp}, "colcommitts12_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Int64}}, "colint13_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Float64}}, "colfloat14_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Numeric}}, "colnumericarray15_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Bool}}, "colboolarray16_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.String}}, "colstringarray17_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.String}}, "colstringmaxarray18_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Bytes}}, "colbytesarray19_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Bytes}}, "colbytesmaxarray20_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Date}}, "coldatearray21_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Timestamp}}, "coltimestamparray22_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Json}}, "coljsonarray23_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Bytes}, "colcomputed24_2_0_"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Bytes}, "asc25_2_0_")
            }, new List<object[]>
                {
                    new object[]
                    {
                        row.ColInt64,
                        row.ColFloat64,
                        row.ColNumeric.Value,
                        row.ColBool,
                        row.ColString,
                        row.ColStringMax,
                        row.ColBytes,
                        row.ColBytesMax,
                        row.ColDate.ToDateTime(),
                        row.ColTimestamp,
                        row.ColJson.Json,
                        row.ColCommitTs,
                        row.ColInt64Array.Array,
                        row.ColFloat64Array.Array,
                        row.ColNumericArray.Array,
                        row.ColBoolArray.Array,
                        row.ColStringArray.Array,
                        row.ColStringMaxArray.Array,
                        row.ColBytesArray.Array,
                        row.ColBytesMaxArray.Array,
                        row.ColDateArray.Array,
                        row.ColTimestampArray.Array,
                        row.ColJsonArray.Array,
                        row.ASC,
                        row.ColComputed
                    },
                }
            );
        }
    }
}