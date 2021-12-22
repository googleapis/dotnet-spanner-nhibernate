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

using Google.Cloud.Spanner.Connection.MockServer;
using Google.Cloud.Spanner.NHibernate.Tests.Entities;
using Google.Cloud.Spanner.V1;
using NHibernate;
using NHibernate.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;
using Environment = NHibernate.Cfg.Environment;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    public class CollectionMappingTests : IClassFixture<NHibernateMockServerFixture>
    {
        private readonly NHibernateMockServerFixture _fixture;

        private readonly ISessionFactory _sessionFactoryForMutations;

        public CollectionMappingTests(NHibernateMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
            var cfg = _fixture.Configuration;
            cfg.Properties[Environment.UseSqlComments] = "false";
            cfg.Properties[Environment.BatchVersionedData] = "true";
            _sessionFactoryForMutations = cfg.BuildSessionFactory();
        }

        [Fact]
        public async Task GetBandMembers()
        {
            var b = new Band { BandId = 100L, Name = "Some band" };
            var membersSql = GetBandMembersSql();
            AddMembersResults(membersSql, b,
                new []
                {
                    new Singer { SingerId = 1L, FirstName = "Pete", LastName = "Allison", FullName = "Pete Allison" },
                    new Singer { SingerId = 2L, FirstName = "Alice",LastName = "Morrison", FullName = "Alice Morrison" },
                }
            );
            var sql = AddBandResult(GetBandSql(), b);
            
            using var session = _fixture.SessionFactory.OpenSession();
            var band = await session.GetAsync<Band>(b.BandId);
            
            Assert.Collection(band.Members,
                singer => Assert.Equal("Pete Allison", singer.FullName),
                singer => Assert.Equal("Alice Morrison", singer.FullName)
            );
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests,
                request => Assert.Equal(sql, request.Sql),
                request => Assert.Equal(membersSql, request.Sql)
            );
        }

        [Fact]
        public async Task AddBandWithMembers()
        {
            var selectSingerSql = GetQuerySingersSql();
            AddSingersResults(selectSingerSql, new[]
            {
                new Singer{ SingerId = 1L, FirstName = "Pete", LastName = "Allison", FullName = "Pete Allison"},
                new Singer{ SingerId = 2L, FirstName = "Alice", LastName = "Morrison", FullName = "Alice Morrison"},
            });
            var insertBandSql = "INSERT INTO Band (Name, BandId) VALUES (@p0, @p1)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertBandSql, StatementResult.CreateUpdateCount(1L));
            var insertMemberSql = "INSERT INTO Members (band_key, elt) VALUES (@p0, @p1)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertMemberSql, StatementResult.CreateUpdateCount(1L));
            
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            var singers = await session
                .Query<Singer>()
                .ToListAsync();
            var band = new Band
            {
                BandId = 1L,
                Name = "New band",
                Members = new List<Singer>(singers),
            };
            await session.SaveAsync(band);
            await transaction.CommitAsync();

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests,
                request => Assert.Equal(selectSingerSql, request.Sql),
                request => Assert.Equal(insertBandSql, request.Sql)
            );
            var batchRequests = _fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>();
            Assert.Collection(batchRequests, request =>
            {
                Assert.Collection(request.Statements,
                    statement => Assert.Equal(insertMemberSql, statement.Sql),
                    statement => Assert.Equal(insertMemberSql, statement.Sql)
                );
            });
        }

        [Fact]
        public async Task UpdateBandMembers()
        {
            var b = new Band { BandId = 100L, Name = "Some band" };
            var membersSql = GetBandMembersSql();
            AddMembersResults(membersSql, b,
                new []
                {
                    new Singer { SingerId = 1L, FirstName = "Pete", LastName = "Allison", FullName = "Pete Allison" },
                    new Singer { SingerId = 2L, FirstName = "Alice", LastName = "Morrison", FullName = "Alice Morrison"},
                }
            );
            var selectBandSql = AddBandResult(GetBandSql(), b);
            var selectSingersSql = GetQuerySingersSql();
            AddSingersResults(selectSingersSql, new[]
            {
                new Singer { SingerId = 3L, FirstName = "Naomi", LastName = "Henderson", FullName = "Naomi Henderson"},
            });
            var insertMemberSql = "INSERT INTO Members (band_key, elt) VALUES (@p0, @p1)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertMemberSql, StatementResult.CreateUpdateCount(1L));
            var deleteMembersSql = "DELETE FROM Members WHERE band_key = @p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(deleteMembersSql, StatementResult.CreateUpdateCount(1L));
            
            // Load a band and update the members of the band.
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            var band = await session.GetAsync<Band>(b.BandId);
            var singers = await session
                .Query<Singer>()
                .ToListAsync();
            singers.ForEach(band.Members.Add);
            await session.UpdateAsync(band);
            await transaction.CommitAsync();

            // The following list of requests shows why one should consider carefully whether one really wants to use a
            // many-to-many relationship that is directly managed by NHibernate. Any changes to the collection will
            // cause NHibernate to first delete all elements in the collection, and then re-insert them using individual
            // DML statements. The Spanner driver will batch these inserts together into one BatchDML request, but it
            // still means quite a lot of DML requests to just add one element to the collection.
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests,
                request => Assert.Equal(selectBandSql, request.Sql),
                request => Assert.Equal(selectSingersSql, request.Sql),
                request => Assert.Equal(membersSql, request.Sql),
                request => Assert.Equal(deleteMembersSql, request.Sql)
            );
            var batchRequests = _fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>();
            Assert.Collection(batchRequests, request =>
            {
                Assert.Collection(request.Statements,
                    statement => Assert.Equal(insertMemberSql, statement.Sql),
                    statement => Assert.Equal(insertMemberSql, statement.Sql),
                    statement => Assert.Equal(insertMemberSql, statement.Sql)
                );
            });
        }

        [Fact]
        public async Task DeleteBandMembers()
        {
            var b = new Band { BandId = 100L, Name = "Some band" };
            var membersSql = GetBandMembersSql();
            AddMembersResults(membersSql, b,
                new []
                {
                    new Singer { SingerId = 1L, FirstName = "Pete", LastName = "Allison", FullName = "Pete Allison" },
                    new Singer { SingerId = 2L, FirstName = "Alice", LastName = "Morrison", FullName = "Alice Morrison"},
                    new Singer { SingerId = 3L, FirstName = "Naomi", LastName = "Henderson", FullName = "Naomi Henderson"},
                }
            );
            var selectBandSql = AddBandResult(GetBandSql(), b);
            var selectSingersSql = GetQuerySingersSql();
            AddSingersResults(selectSingersSql, new[]
            {
                new Singer{ SingerId = 3L, FirstName = "Naomi", LastName = "Henderson", FullName = "Naomi Henderson"},
            });
            var insertMemberSql = "INSERT INTO Members (band_key, elt) VALUES (@p0, @p1)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertMemberSql, StatementResult.CreateUpdateCount(1L));
            var deleteMembersSql = "DELETE FROM Members WHERE band_key = @p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(deleteMembersSql, StatementResult.CreateUpdateCount(1L));
            
            // Load a band and remove one member from the band.
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            var band = await session.GetAsync<Band>(b.BandId);
            band.Members.RemoveAt(band.Members.Count - 1);
            await session.UpdateAsync(band);
            await transaction.CommitAsync();

            // The following list of requests shows why one should consider carefully whether one really wants to use a
            // many-to-many relationship that is directly managed by NHibernate. Any changes to the collection will
            // cause NHibernate to first delete all elements in the collection, and then re-insert them using individual
            // DML statements. The Spanner driver will batch these inserts together into one BatchDML request, but it
            // still means quite a lot of DML requests to just remove one element from the collection.
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests,
                request => Assert.Equal(selectBandSql, request.Sql),
                request => Assert.Equal(membersSql, request.Sql),
                request => Assert.Equal(deleteMembersSql, request.Sql)
            );
            var batchRequests = _fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>();
            Assert.Collection(batchRequests, request =>
            {
                Assert.Collection(request.Statements,
                    statement => Assert.Equal(insertMemberSql, statement.Sql),
                    statement => Assert.Equal(insertMemberSql, statement.Sql)
                );
            });
        }

        [Fact]
        public async Task DeleteBand()
        {
            var b = new Band { BandId = 100L, Name = "Some band" };
            var selectBandSql = AddBandResult(GetBandSql(), b);
            var deleteMembersSql = "DELETE FROM Members WHERE band_key = @p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(deleteMembersSql, StatementResult.CreateUpdateCount(3L));
            var deleteBandSql = "DELETE FROM Band WHERE BandId = @p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(deleteBandSql, StatementResult.CreateUpdateCount(1L));
            
            // Load a band and delete it. All entries in the Members table should also automatically be deleted.
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            var band = await session.GetAsync<Band>(b.BandId);
            await session.DeleteAsync(band);
            await transaction.CommitAsync();

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests,
                request => Assert.Equal(selectBandSql, request.Sql),
                request => Assert.Equal(deleteMembersSql, request.Sql),
                request => Assert.Equal(deleteBandSql, request.Sql)
            );
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
        }

        [Fact]
        public async Task AddBandWithMembersUsingMutations()
        {
            var selectSingerSql = GetQuerySingersSql();
            AddSingersResults(selectSingerSql, new[]
            {
                new Singer{ SingerId = 1L, FirstName = "Pete", LastName = "Allison", FullName = "Pete Allison"},
                new Singer{ SingerId = 2L, FirstName = "Alice", LastName = "Morrison", FullName = "Alice Morrison"},
            });
            using var session = _sessionFactoryForMutations.OpenSession();
            using var transaction = session.BeginTransaction(MutationUsage.Always);
            var singers = await session
                .Query<Singer>()
                .ToListAsync();
            var band = new Band
            {
                BandId = 1L,
                Name = "New band",
                Members = new List<Singer>(singers),
            };
            await session.SaveAsync(band);
            // This transaction would mix DML and mutations, which is not supported, as there is generally no guarantee
            // that they would be applied in the correct order.
            await Assert.ThrowsAsync<HibernateException>(() => transaction.CommitAsync());
            
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<CommitRequest>());
        }

        [Fact]
        public async Task AddBandWithoutMembersUsingMutations()
        {
            using var session = _sessionFactoryForMutations.OpenSession();
            using var transaction = session.BeginTransaction(MutationUsage.Always);
            var band = new Band
            {
                BandId = 1L,
                Name = "New band",
            };
            await session.SaveAsync(band);
            await transaction.CommitAsync();

            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits, commit =>
            {
                Assert.Collection(commit.Mutations, mutation =>
                {
                    Assert.Equal("Band", mutation.Insert.Table);
                    Assert.Collection(mutation.Insert.Values, row =>
                    {
                        Assert.Collection(row.Values,
                            column => Assert.Equal("New band", column.StringValue),
                            column => Assert.Equal("1", column.StringValue)
                        );
                    });
                    Assert.Collection(mutation.Insert.Columns,
                        column => Assert.Equal("Name", column),
                        column => Assert.Equal("BandId", column)
                    );
                });
            });
        }

        [Fact]
        public async Task UpdateBandMembersUsingMutations()
        {
            var b = new Band { BandId = 100L, Name = "Some band" };
            var membersSql = GetBandMembersSql();
            AddMembersResults(membersSql, b,
                new []
                {
                    new Singer { SingerId = 1L, FirstName = "Pete", LastName = "Allison", FullName = "Pete Allison" },
                    new Singer { SingerId = 2L, FirstName = "Alice", LastName = "Morrison", FullName = "Alice Morrison"},
                }
            );
            var selectBandSql = AddBandResult(GetBandSql(), b);
            var selectSingersSql = GetQuerySingersSql();
            AddSingersResults(selectSingersSql, new[]
            {
                new Singer { SingerId = 3L, FirstName = "Naomi", LastName = "Henderson", FullName = "Naomi Henderson"},
            });
            
            // Load a band and update the members of the band.
            using var session = _sessionFactoryForMutations.OpenSession();
            using var transaction = session.BeginTransaction(MutationUsage.Always);
            var band = await session.GetAsync<Band>(b.BandId);
            var singers = await session
                .Query<Singer>()
                .ToListAsync();
            singers.ForEach(band.Members.Add);
            await session.UpdateAsync(band);
            await Assert.ThrowsAsync<HibernateException>(() => transaction.CommitAsync());
            
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<CommitRequest>());
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
            Assert.Collection(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request => Assert.Equal(selectBandSql, request.Sql),
                request => Assert.Equal(selectSingersSql, request.Sql),
                request => Assert.Equal(membersSql, request.Sql)
            );
        }

        [Fact]
        public async Task DeleteBandMembersUsingMutations()
        {
            var b = new Band { BandId = 100L, Name = "Some band" };
            var membersSql = GetBandMembersSql();
            AddMembersResults(membersSql, b,
                new []
                {
                    new Singer { SingerId = 1L, FirstName = "Pete", LastName = "Allison", FullName = "Pete Allison" },
                    new Singer { SingerId = 2L, FirstName = "Alice", LastName = "Morrison", FullName = "Alice Morrison"},
                    new Singer { SingerId = 3L, FirstName = "Naomi", LastName = "Henderson", FullName = "Naomi Henderson"},
                }
            );
            var selectBandSql = AddBandResult(GetBandSql(), b);
            var selectSingersSql = GetQuerySingersSql();
            AddSingersResults(selectSingersSql, new[]
            {
                new Singer{ SingerId = 3L, FirstName = "Naomi", LastName = "Henderson", FullName = "Naomi Henderson"},
            });
            
            // Load a band and remove one member from the band.
            using var session = _sessionFactoryForMutations.OpenSession();
            using var transaction = session.BeginTransaction(MutationUsage.Always);
            var band = await session.GetAsync<Band>(b.BandId);
            band.Members.RemoveAt(band.Members.Count - 1);
            await session.UpdateAsync(band);
            await Assert.ThrowsAsync<HibernateException>(() => transaction.CommitAsync());

            Assert.Empty(_fixture.SpannerMock.Requests.OfType<CommitRequest>());
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests,
                request => Assert.Equal(selectBandSql, request.Sql),
                request => Assert.Equal(membersSql, request.Sql)
            );
        }

        [Fact]
        public async Task DeleteBandUsingMutations()
        {
            var b = new Band { BandId = 100L, Name = "Some band" };
            var selectBandSql = AddBandResult(GetBandSql(), b);
            
            // Load a band and delete it. All entries in the Members table should also automatically be deleted.
            using var session = _sessionFactoryForMutations.OpenSession();
            using var transaction = session.BeginTransaction(MutationUsage.Always);
            var band = await session.GetAsync<Band>(b.BandId);
            await session.DeleteAsync(band);
            await Assert.ThrowsAsync<HibernateException>(() => transaction.CommitAsync());

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests,
                request => Assert.Equal(selectBandSql, request.Sql)
            );
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<CommitRequest>());
        }

        private string GetBandSql() =>
            "SELECT band0_.BandId as bandid1_10_0_, band0_.Name as name2_10_0_ FROM Band band0_ WHERE band0_.BandId=@p0";

        private string AddBandResult(string sql, Band band) =>
            AddBandResults(sql, new List<object[]>
            {
                new object[] { band.BandId, band.Name },
            });

        private string AddBandResults(string sql, IEnumerable<object[]> rows)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<TypeCode, string>>
                {
                    Tuple.Create(TypeCode.Int64, "bandid1_10_0_"),
                    Tuple.Create(TypeCode.String, "name2_10_0_"),
                }, rows));
            return sql;
        }

        private static string GetBandMembersSql() =>
            "SELECT members0_.band_key as band1_11_1_, members0_.elt as elt2_11_1_, singer1_.SingerId as singerid1_0_0_, singer1_.FirstName as firstname2_0_0_, singer1_.LastName as lastname3_0_0_, singer1_.FullName as fullname4_0_0_, singer1_.BirthDate as birthdate5_0_0_, singer1_.Picture as picture6_0_0_ " +
            "FROM Members members0_ " +
            "left outer join Singer singer1_ on members0_.elt=singer1_.SingerId " +
            "WHERE members0_.band_key=@p0";
        
        private void AddMembersResults(string sql, Band band, IEnumerable<Singer> singers) =>
            AddBandMembersResults(sql, singers.Select(s => 
                new object[] { band.BandId, s.SingerId, s.SingerId, s.FirstName, s.LastName, s.FullName, s.BirthDate?.ToString(CultureInfo.InvariantCulture), s.Picture == null ? null : Convert.ToBase64String(s.Picture) }
            ).ToList());
        
        private void AddBandMembersResults(string sql, IEnumerable<object[]> rows) =>
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<TypeCode, string>>
                {
                    Tuple.Create(TypeCode.Int64, "band1_11_1_"),
                    Tuple.Create(TypeCode.Int64, "elt2_11_1_"), // `elt` is the default name for the child (element)
                    Tuple.Create(TypeCode.Int64, "singerid1_0_0_"),
                    Tuple.Create(TypeCode.String, "firstname2_0_0_"),
                    Tuple.Create(TypeCode.String, "lastname3_0_0_"),
                    Tuple.Create(TypeCode.String, "fullname4_0_0_"),
                    Tuple.Create(TypeCode.Date, "birthdate5_0_0_"),
                    Tuple.Create(TypeCode.Bytes, "picture6_0_0_"),
                }, rows));

        private static string GetQuerySingersSql() =>
            "select singer0_.SingerId as singerid1_0_, singer0_.FirstName as firstname2_0_, singer0_.LastName as lastname3_0_, singer0_.FullName as fullname4_0_, singer0_.BirthDate as birthdate5_0_, singer0_.Picture as picture6_0_ from Singer singer0_";
        
        private void AddSingersResults(string sql, IEnumerable<Singer> singers) =>
            AddSingersResults(sql, singers.Select(s => 
                new object[] { s.SingerId, s.FirstName, s.LastName, s.FullName, s.BirthDate?.ToString(CultureInfo.InvariantCulture), s.Picture == null ? null : Convert.ToBase64String(s.Picture) }
            ).ToList());
        
        private void AddSingersResults(string sql, IEnumerable<object[]> rows) =>
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<TypeCode, string>>
                {
                    Tuple.Create(TypeCode.Int64, "singerid1_0_"),
                    Tuple.Create(TypeCode.String, "firstname2_0_"),
                    Tuple.Create(TypeCode.String, "lastname3_0_"),
                    Tuple.Create(TypeCode.String, "fullname4_0_"),
                    Tuple.Create(TypeCode.Date, "birthdate5_0_"),
                    Tuple.Create(TypeCode.Bytes, "picture6_0_"),
                }, rows));
    }
}