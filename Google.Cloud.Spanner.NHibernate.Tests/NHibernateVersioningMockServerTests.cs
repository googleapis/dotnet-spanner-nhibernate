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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Cloud.Spanner.Connection.MockServer;
using Google.Cloud.Spanner.NHibernate.Tests.Entities;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using NHibernate;
using Xunit;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    /// <summary>
    /// Tests optimistic concurrency using an in-mem Spanner mock server.
    /// </summary>
    public class NHibernateVersioningMockServerTests : IClassFixture<NHibernateMockServerFixture>
    {
        private readonly NHibernateMockServerFixture _fixture;

        public NHibernateVersioningMockServerTests(NHibernateMockServerFixture service)
        {
            _fixture = service;
            service.SpannerMock.Reset();
        }

        [Fact]
        public async Task VersionNumberIsAutomaticallyGeneratedOnInsertAndUpdate()
        {
            var insertSql = "INSERT INTO SingerWithVersion (Version, FirstName, LastName, SingerId) VALUES (@p0, @p1, @p2, @p3)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));

            var singer = new SingerWithVersion { SingerId = 1L, FirstName = "Pete", LastName = "Allison" };

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    await session.SaveAsync(singer);
                    await transaction.CommitAsync();
                }
            }

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(insertSql, request.Sql);
                    // Verify that the version is 1.
                    Assert.Equal(Value.ForString("1"), request.Params.Fields["p0"]);
                    Assert.Equal(Value.ForString("Pete"), request.Params.Fields["p1"]);
                    Assert.Equal(Value.ForString("Allison"), request.Params.Fields["p2"]);
                    Assert.Equal(Value.ForString("1"), request.Params.Fields["p3"]);
                }
            );

            _fixture.SpannerMock.Reset();
            // Update the singer and verify that the version number is included in the WHERE clause and is updated.
            var updateSql = "UPDATE SingerWithVersion SET Version = @p0, FirstName = @p1, LastName = @p2 WHERE SingerId = @p3 AND Version = @p4";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));

            singer.LastName = "Peterson - Allison";

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    await session.SaveOrUpdateAsync(singer);
                    await transaction.CommitAsync();
                }
            }

            Assert.Collection(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(), updateQueryRequest =>
                {
                    Assert.NotNull(updateQueryRequest);

                    Assert.Equal(updateSql, updateQueryRequest.Sql);
                    // Verify that the version that is set is 2.
                    Assert.Equal(Value.ForString("2"), updateQueryRequest.Params.Fields["p0"]);

                    Assert.Equal(Value.ForString("Pete"), updateQueryRequest.Params.Fields["p1"]);
                    Assert.Equal(Value.ForString("Peterson - Allison"), updateQueryRequest.Params.Fields["p2"]);
                    Assert.Equal(Value.ForString("1"), updateQueryRequest.Params.Fields["p3"]);
                    // Verify that the version that is checked is 1.
                    Assert.Equal(Value.ForString("1"), updateQueryRequest.Params.Fields["p4"]);
                });
        }

        [Fact]
        public async Task UpdateFailsIfVersionNumberChanged()
        {
            var updateSql = "UPDATE SingerWithVersion SET Version = @p0, FirstName = @p1, LastName = @p2 WHERE SingerId = @p3 AND Version = @p4";
            var selectSql = "SELECT singerwith0_.SingerId as singerid1_4_0_, singerwith0_.Version as version2_4_0_, singerwith0_.FirstName as firstname3_4_0_, singerwith0_.LastName as lastname4_4_0_ FROM SingerWithVersion singerwith0_ WHERE singerwith0_.SingerId=@p0";
            // Set the update count to 0 to indicate that the row was not found.
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(0L));
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectSql, StatementResult.CreateResultSet(new List<Tuple<V1.TypeCode, string>>
            {
                Tuple.Create(TypeCode.Int64, "singerid1_4_0_"),
                Tuple.Create(TypeCode.Int64, "version2_4_0_"),
                Tuple.Create(TypeCode.String, "firstname3_4_0_"),
                Tuple.Create(TypeCode.String, "lastname4_4_0_")
            }, new List<object[]>
            {
                new object[]{"1", "1", "Pete", "Allison"}
            }));

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    var singer = await session.LoadAsync<SingerWithVersion>(1L);
                    singer.LastName = "Allison - Peterson";
                    await Assert.ThrowsAsync<StaleStateException>(() => session.FlushAsync());
                    // Update the update count to 1 to simulate a resolved version conflict.
                    _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));
                    await transaction.CommitAsync();
                }
            }
        }

        [CombinatorialData]
        [Theory]
        public async Task VersionNumberIsAutomaticallyGeneratedOnInsertAndUpdateUsingMutations(bool useAsync)
        {
            var singer = new SingerWithVersion { SingerId = 50L, FirstName = "Pete", LastName = "Allison" };
            using (var session = _fixture.SessionFactoryUsingMutations.OpenSession())
            {
                using (var transaction = session.BeginTransaction(MutationUsage.Always))
                {
                    if (useAsync)
                    {
                        await session.SaveAsync(singer);
                        await transaction.CommitAsync();
                    }
                    else
                    {
                        session.Save(singer);
                        transaction.Commit();
                        await Task.CompletedTask;
                    }
                }
            }

            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<CommitRequest>(),
                r =>
                {
                    Assert.Collection(
                        r.Mutations,
                        mutation =>
                        {
                            Assert.Equal(Mutation.OperationOneofCase.Insert, mutation.OperationCase);
                            Assert.Equal("SingerWithVersion", mutation.Insert.Table);
                            Assert.Collection(
                                mutation.Insert.Columns,
                                column => Assert.Equal("Version", column),
                                column => Assert.Equal("FirstName", column),
                                column => Assert.Equal("LastName", column),
                                column => Assert.Equal("SingerId", column)
                            );
                            Assert.Collection(
                                mutation.Insert.Values,
                                row => Assert.Collection(
                                    row.Values,
                                    value => Assert.Equal("1", value.StringValue),
                                    value => Assert.Equal("Pete", value.StringValue),
                                    value => Assert.Equal("Allison", value.StringValue),
                                    value => Assert.Equal("50", value.StringValue)
                                )
                            );
                        }
                    );
                });

            _fixture.SpannerMock.Reset();
            var checkVersionSql = "SELECT 1 AS C FROM SingerWithVersion WHERE SingerId = @p0 AND Version = @p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(checkVersionSql, StatementResult.CreateResultSet(
                new []{new Tuple<TypeCode, string>(TypeCode.Int64, "C")},
                new []{ new object[] {1L}}));
            
            // Update the singer and verify that a SELECT statement that checks the version number is executed.
            singer.LastName = "Peterson - Allison";
            using (var session = _fixture.SessionFactoryUsingMutations.OpenSession())
            {
                using (var transaction = session.BeginTransaction(MutationUsage.Always))
                {
                    await session.SaveOrUpdateAsync(singer);
                    await transaction.CommitAsync();
                }
            }

            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
            Assert.Collection(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(checkVersionSql, request.Sql);
                    Assert.Collection(request.Params.Fields,
                        p => Assert.Equal("50", p.Value.StringValue), // SingerId
                        p => Assert.Equal("1", p.Value.StringValue) // Version
                    );
                });
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<CommitRequest>(),
                r =>
                {
                    Assert.Collection(
                        r.Mutations,
                        mutation =>
                        {
                            Assert.Equal(Mutation.OperationOneofCase.Update, mutation.OperationCase);
                            Assert.Equal("SingerWithVersion", mutation.Update.Table);
                            Assert.Collection(
                                mutation.Update.Columns,
                                column => Assert.Equal("Version", column),
                                column => Assert.Equal("FirstName", column),
                                column => Assert.Equal("LastName", column),
                                column => Assert.Equal("SingerId", column)
                            );
                            Assert.Collection(
                                mutation.Update.Values,
                                row => Assert.Collection(
                                    row.Values,
                                    // Verify that the version that is set is 2.
                                    value => Assert.Equal("2", value.StringValue),
                                    value => Assert.Equal("Pete", value.StringValue),
                                    value => Assert.Equal("Peterson - Allison", value.StringValue),
                                    value => Assert.Equal("50", value.StringValue)
                                )
                            );
                        }
                    );
                }
            );
        }

        [CombinatorialData]
        [Theory]
        public async Task UpdateFailsIfVersionNumberChangedUsingMutations(bool useAsync)
        {
            var checkVersionSql = "SELECT 1 AS C FROM SingerWithVersion WHERE SingerId = @p0 AND Version = @p1";
            // Add an empty result for the version check query.
            _fixture.SpannerMock.AddOrUpdateStatementResult(checkVersionSql, StatementResult.CreateResultSet(
                new List<Tuple<TypeCode, string>> {Tuple.Create(TypeCode.Int64, "C")}, new List<object[]>()));

            var selectSql = "/* load Google.Cloud.Spanner.NHibernate.Tests.Entities.SingerWithVersion */ SELECT singerwith0_.SingerId as singerid1_4_0_, singerwith0_.Version as version2_4_0_, singerwith0_.FirstName as firstname3_4_0_, singerwith0_.LastName as lastname4_4_0_ FROM SingerWithVersion singerwith0_ WHERE singerwith0_.SingerId=@p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectSql, StatementResult.CreateResultSet(new List<Tuple<V1.TypeCode, string>>
            {
                Tuple.Create(TypeCode.Int64, "singerid1_4_0_"),
                Tuple.Create(TypeCode.Int64, "version2_4_0_"),
                Tuple.Create(TypeCode.String, "firstname3_4_0_"),
                Tuple.Create(TypeCode.String, "lastname4_4_0_")
            }, new List<object[]>
            {
                new object[]{"50", "1", "Pete", "Allison"}
            }));

            using (var session = _fixture.SessionFactoryUsingMutations.OpenSession())
            {
                using (var transaction = session.BeginTransaction(MutationUsage.Always))
                {
                    if (useAsync)
                    {
                        var singer = await session.LoadAsync<SingerWithVersion>(50L);
                        singer.LastName = "Allison - Peterson";
                        // This will fail because no result is found by the check version SELECT statement.
                        await Assert.ThrowsAsync<StaleStateException>(() => transaction.CommitAsync());
                    }
                    else
                    {
                        var singer = session.Load<SingerWithVersion>(50L);
                        singer.LastName = "Allison - Peterson";
                        // This will fail because no result is found by the check version SELECT statement.
                        Assert.Throws<StaleStateException>(() => transaction.Commit());
                    }
                }
            }

            // Update the result of the check version SELECT statement to simulate a resolved version conflict.
            _fixture.SpannerMock.AddOrUpdateStatementResult(checkVersionSql, StatementResult.CreateResultSet(
                new []{new Tuple<TypeCode, string>(TypeCode.Int64, "C")},
                new []{ new object[] {1L}}));
            using (var session = _fixture.SessionFactoryUsingMutations.OpenSession())
            {
                using (var transaction = session.BeginTransaction(MutationUsage.Always))
                {
                    if (useAsync)
                    {
                        var singer = await session.LoadAsync<SingerWithVersion>(50L);
                        singer.LastName = "Allison - Peterson";
                        // This will now succeed because a result is found by the version check.
                        await transaction.CommitAsync();
                    }
                    else
                    {
                        var singer = session.Load<SingerWithVersion>(50L);
                        singer.LastName = "Allison - Peterson";
                        // This will now succeed because a result is found by the version check.
                        transaction.Commit();
                        await Task.CompletedTask;
                    }
                }
            }

            // There are two transaction attempts that execute the same SELECT statements.
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests,
                request => Assert.Equal(request.Sql, selectSql),
                request =>
                {
                    Assert.Equal(request.Sql, checkVersionSql);
                    Assert.Collection(request.Params.Fields,
                        p => Assert.Equal("50", p.Value.StringValue),
                        p => Assert.Equal("1", p.Value.StringValue)
                    );
                },
                request => Assert.Equal(request.Sql, selectSql),
                request =>
                {
                    Assert.Equal(request.Sql, checkVersionSql);
                    Assert.Collection(request.Params.Fields,
                        p => Assert.Equal("50", p.Value.StringValue),
                        p => Assert.Equal("1", p.Value.StringValue)
                    );
                }
            );
            // Only one of the two transactions will commit successfully.
            var commitRequests = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commitRequests, commit =>
            {
                Assert.Collection(commit.Mutations, mutation =>
                {
                    Assert.Equal(Mutation.OperationOneofCase.Update, mutation.OperationCase);
                    Assert.Equal("SingerWithVersion", mutation.Update.Table);
                    Assert.Collection(mutation.Update.Columns,
                        c => Assert.Equal("Version", c),
                        c => Assert.Equal("LastName", c),
                        c => Assert.Equal("SingerId", c)
                    );
                    Assert.Collection(mutation.Update.Values, row => Assert.Collection(row.Values,
                        c => Assert.Equal("2", c.StringValue),
                        c => Assert.Equal("Allison - Peterson", c.StringValue),
                        c => Assert.Equal("50", c.StringValue)
                    ));
                });
            });
        }

        [CombinatorialData]
        [Theory]
        public async Task DeleteFailsIfVersionNumberChangedUsingMutations(bool useAsync)
        {
            var checkVersionSql = "SELECT 1 AS C FROM SingerWithVersion WHERE SingerId = @p0 AND Version = @p1";
            // Add an empty result for the version check query.
            _fixture.SpannerMock.AddOrUpdateStatementResult(checkVersionSql, StatementResult.CreateResultSet(
                new List<Tuple<TypeCode, string>> {Tuple.Create(TypeCode.Int64, "C")}, new List<object[]>()));

            var selectSql = "/* load Google.Cloud.Spanner.NHibernate.Tests.Entities.SingerWithVersion */ SELECT singerwith0_.SingerId as singerid1_4_0_, singerwith0_.Version as version2_4_0_, singerwith0_.FirstName as firstname3_4_0_, singerwith0_.LastName as lastname4_4_0_ FROM SingerWithVersion singerwith0_ WHERE singerwith0_.SingerId=@p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectSql, StatementResult.CreateResultSet(new List<Tuple<V1.TypeCode, string>>
            {
                Tuple.Create(TypeCode.Int64, "singerid1_4_0_"),
                Tuple.Create(TypeCode.Int64, "version2_4_0_"),
                Tuple.Create(TypeCode.String, "firstname3_4_0_"),
                Tuple.Create(TypeCode.String, "lastname4_4_0_")
            }, new List<object[]>
            {
                new object[]{"50", "1", "Pete", "Allison"}
            }));

            using (var session = _fixture.SessionFactoryUsingMutations.OpenSession())
            {
                using (var transaction = session.BeginTransaction(MutationUsage.Always))
                {
                    if (useAsync)
                    {
                        var singer = await session.LoadAsync<SingerWithVersion>(50L);
                        await session.DeleteAsync(singer);
                        // This will fail because no result is found by the check version SELECT statement.
                        await Assert.ThrowsAsync<StaleStateException>(() => transaction.CommitAsync());
                    }
                    else
                    {
                        var singer = session.Load<SingerWithVersion>(50L);
                        session.Delete(singer);
                        // This will fail because no result is found by the check version SELECT statement.
                        Assert.Throws<StaleStateException>(() => transaction.Commit());
                        await Task.CompletedTask;
                    }
                }
            }

            // Update the result of the check version SELECT statement to simulate a resolved version conflict.
            _fixture.SpannerMock.AddOrUpdateStatementResult(checkVersionSql, StatementResult.CreateResultSet(
                new []{new Tuple<TypeCode, string>(TypeCode.Int64, "C")},
                new []{ new object[] {1L}}));
            using (var session = _fixture.SessionFactoryUsingMutations.OpenSession())
            {
                using (var transaction = session.BeginTransaction(MutationUsage.Always))
                {
                    if (useAsync)
                    {
                        var singer = await session.LoadAsync<SingerWithVersion>(50L);
                        await session.DeleteAsync(singer);
                        // This will now succeed because a result is found by the version check.
                        await transaction.CommitAsync();
                    }
                    else
                    {
                        var singer = session.Load<SingerWithVersion>(50L);
                        session.Delete(singer);
                        // This will now succeed because a result is found by the version check.
                        transaction.Commit();
                    }
                }
            }

            // There are two transaction attempts that execute the same SELECT statements.
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests,
                request => Assert.Equal(request.Sql, selectSql),
                request =>
                {
                    Assert.Equal(request.Sql, checkVersionSql);
                    Assert.Collection(request.Params.Fields,
                        p => Assert.Equal("50", p.Value.StringValue),
                        p => Assert.Equal("1", p.Value.StringValue)
                    );
                },
                request => Assert.Equal(request.Sql, selectSql),
                request =>
                {
                    Assert.Equal(request.Sql, checkVersionSql);
                    Assert.Collection(request.Params.Fields,
                        p => Assert.Equal("50", p.Value.StringValue),
                        p => Assert.Equal("1", p.Value.StringValue)
                    );
                }
            );
            // Only one of the two transactions will commit successfully.
            var commitRequests = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commitRequests, commit =>
            {
                Assert.Collection(commit.Mutations, mutation =>
                {
                    Assert.Equal(Mutation.OperationOneofCase.Delete, mutation.OperationCase);
                    Assert.Equal("SingerWithVersion", mutation.Delete.Table);
                    Assert.Collection(mutation.Delete.KeySet.Keys, key => Assert.Collection(key.Values,
                        c => Assert.Equal("50", c.StringValue)
                    ));
                });
            });
        }
    }
}
