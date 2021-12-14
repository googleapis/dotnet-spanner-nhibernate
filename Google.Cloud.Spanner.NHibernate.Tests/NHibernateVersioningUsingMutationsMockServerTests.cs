﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Cloud.Spanner.Connection.MockServer;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.NHibernate.Tests.Entities;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using NHibernate;
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.Tests
{

    /// <summary>
    /// Tests optimistic concurrency with mutations using an in-mem Spanner mock server.
    /// </summary>
    public class NHibernateVersioningUsingMutationsMockServerTests : IClassFixture<NHibernateMockServerFixture>
    {

        private readonly NHibernateMockServerFixture _fixture;

        public NHibernateVersioningUsingMutationsMockServerTests(NHibernateMockServerFixture service)
        {
            _fixture = service;
            service.SpannerMock.Reset();
        }

        [Fact]
        public async Task VersionNumberIsAutomaticallyGeneratedOnInsertAndUpdate()
        {
            var singer = new SingerWithVersion { SingerId = 1L, FirstName = "Pete", LastName = "Allison" };

            using (var session = _fixture.SessionFactoryUsingMutations.OpenSession())
            {
                using (var transaction = session.BeginTransaction(MutationUsage.Always))
                {
                    await session.SaveAsync(singer);
                    await transaction.CommitAsync();
                }
            }

            Assert.Empty(_fixture.SpannerMock.Requests.Where(r => r is ExecuteBatchDmlRequest));
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
                                    value => Assert.Equal("1", value.StringValue)
                                )
                            );
                        }
                    );
                });

            _fixture.SpannerMock.Reset();

            // Update the singer and verify that the version number is included in the WHERE clause and is updated.
            singer.LastName = "Peterson - Allison";

            using (var session = _fixture.SessionFactoryUsingMutations.OpenSession())
            {
                using (var transaction = session.BeginTransaction(MutationUsage.Always))
                {
                    await session.SaveOrUpdateAsync(singer);
                    await transaction.CommitAsync();
                }
            }

            Assert.Empty(_fixture.SpannerMock.Requests.Where(r => r is ExecuteBatchDmlRequest));
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
                                    value => Assert.Equal("1", value.StringValue)
                                )
                            );
                        }
                    );
                }

            );
        }

        [Fact]
        public async Task UpdateFailsIfVersionNumberChanged()
        {
            var updateSql = "/* update Google.Cloud.Spanner.NHibernate.Tests.Entities.SingerWithVersion */ UPDATE SingerWithVersion SET Version = @p0, LastName = @p1 WHERE SingerId = @p2 AND Version = @p3";
            var selectSql = "/* load Google.Cloud.Spanner.NHibernate.Tests.Entities.SingerWithVersion */ SELECT singerwith0_.SingerId as singerid1_4_0_, singerwith0_.Version as version2_4_0_, singerwith0_.FirstName as firstname3_4_0_, singerwith0_.LastName as lastname4_4_0_ FROM SingerWithVersion singerwith0_ WHERE singerwith0_.SingerId=@p0";
            // Set the update count to 0 to indicate that the row was not found.
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(0L));
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectSql, StatementResult.CreateResultSet(new List<Tuple<V1.TypeCode, string>>
            {
                Tuple.Create(V1.TypeCode.Int64, "singerid1_4_0_"),
                Tuple.Create(V1.TypeCode.Int64, "version2_4_0_"),
                Tuple.Create(V1.TypeCode.String, "firstname3_4_0_"),
                Tuple.Create(V1.TypeCode.String, "lastname4_4_0_")
            }, new List<object[]>
            {
                new object[]{"1", "1", "Pete", "Allison"}
            }));

            using (var session = _fixture.SessionFactoryUsingMutations.OpenSession())
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
    }
}