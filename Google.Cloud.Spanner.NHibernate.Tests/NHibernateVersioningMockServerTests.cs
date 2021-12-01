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
                Tuple.Create(V1.TypeCode.Int64, "singerid1_4_0_"),
                Tuple.Create(V1.TypeCode.Int64, "version2_4_0_"),
                Tuple.Create(V1.TypeCode.String, "firstname3_4_0_"),
                Tuple.Create(V1.TypeCode.String, "lastname4_4_0_")
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
                    await Assert.ThrowsAsync<StaleObjectStateException>(() => session.FlushAsync());
                    // Update the update count to 1 to simulate a resolved version conflict.
                    _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));
                    await transaction.CommitAsync();
                }
            }
        }
    }
}
