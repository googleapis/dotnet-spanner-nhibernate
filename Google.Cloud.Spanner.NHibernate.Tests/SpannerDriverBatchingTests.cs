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
using Google.Protobuf.WellKnownTypes;
using NHibernate.Impl;
using NHibernate.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Environment = NHibernate.Cfg.Environment;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    public class SpannerDriverBatchingTests : IClassFixture<NHibernateMockServerFixture>
    {
        private readonly NHibernateMockServerFixture _fixture;

        public SpannerDriverBatchingTests(NHibernateMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        [CombinatorialData]
        [Theory]
        public async Task InsertMultipleAlbums_UsesBatchDml(bool async)
        {
            var insertSql = "INSERT INTO Album (SingerId, Title, ReleaseDate, AlbumId) VALUES (@p0, @p1, @p2, @p3)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));

            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            var album1 = new Album
            {
                AlbumId = 1L,
                Title = "Title 1",
            };
            var album2 = new Album
            {
                AlbumId = 2L,
                Title = "Title 2",
            };
            if (async)
            {
                await session.SaveAsync(album1);
                await session.SaveAsync(album2);
                await transaction.CommitAsync();
            }
            else
            {
                session.Save(album1);
                session.Save(album2);
                transaction.Commit();
            }
            AssertAlbumBatchDmlRequests(insertSql, false);
        }

        [CombinatorialData]
        [Theory]
        public async Task UpdateMultipleAlbums_UsesBatchDml(bool async)
        {
            var updateSql = "UPDATE Album SET SingerId = @p0, Title = @p1, ReleaseDate = @p2 WHERE AlbumId = @p3";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));
            AddQueryAlbumsResults(QueryAllAlbumsSql(),
                new Album { AlbumId = 1L, Title = "My first title" },
                new Album { AlbumId = 2L, Title = "My second title" });

            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            if (async)
            {
                var albums = await session.Query<Album>().ToListAsync();
                albums.ForEach(a => a.Title = $"Title {a.AlbumId}");
                foreach (var album in albums)
                {
                    await session.UpdateAsync(album);
                }
                await transaction.CommitAsync();
            }
            else
            {
                var albums = session.Query<Album>().ToList();
                albums.ForEach(a => a.Title = $"Title {a.AlbumId}");
                albums.ForEach(a => session.Update(a));
                transaction.Commit();
            }
            AssertAlbumBatchDmlRequests(updateSql, true);
        }

        private void AssertAlbumBatchDmlRequests(string sql, bool withSelect)
        {
            var transactionId = _fixture.SpannerMock.Requests.OfType<CommitRequest>().First().TransactionId;
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>(),
                request =>
                {
                    Assert.Equal(transactionId, request.Transaction.Id);
                    Assert.Collection(request.Statements,
                        statement =>
                        {
                            Assert.Equal(sql, statement.Sql);
                            Assert.Collection(statement.Params.Fields,
                                param => Assert.Equal(Value.KindOneofCase.NullValue, param.Value.KindCase),
                                param => Assert.Equal("Title 1", param.Value.StringValue),
                                param => Assert.Equal(Value.KindOneofCase.NullValue, param.Value.KindCase),
                                param => Assert.Equal("1", param.Value.StringValue));
                        },
                        statement =>
                        {
                            Assert.Equal(sql, statement.Sql);
                            Assert.Collection(statement.Params.Fields,
                                param => Assert.Equal(Value.KindOneofCase.NullValue, param.Value.KindCase),
                                param => Assert.Equal("Title 2", param.Value.StringValue),
                                param => Assert.Equal(Value.KindOneofCase.NullValue, param.Value.KindCase),
                                param => Assert.Equal("2", param.Value.StringValue));
                        });
                });
            if (withSelect)
            {
                Assert.Collection(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                    request => Assert.StartsWith("SELECT", request.Sql.ToUpper()));
            }
            else
            {
                Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            }
            Assert.Single(_fixture.SpannerMock.Requests.OfType<CommitRequest>());
        }

        [CombinatorialData]
        [Theory]
        public async Task InsertMultipleAlbumsWithoutTransaction_UsesBatchDml(bool async)
        {
            // This scenario could in theory use Mutations instead of BatchDml. However, NHibernate is
            // not really made to create anything that is not SQL based. In this specific case it would
            // mean implementing a custom EntityPersister and override AbstractEntityPersister.GenerateInsertString.
            // That would mean overriding the heart of NHibernate, and is not worth the effort when compared to the
            // relatively small upside.
            
            var insertSql = "INSERT INTO Album (SingerId, Title, ReleaseDate, AlbumId) VALUES (@p0, @p1, @p2, @p3)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));

            using var session = _fixture.SessionFactory.OpenSession();
            var album1 = new Album
            {
                AlbumId = 1L,
                Title = "Title 1",
            };
            var album2 = new Album
            {
                AlbumId = 2L,
                Title = "Title 2",
            };
            if (async)
            {
                await session.SaveAsync(album1);
                await session.SaveAsync(album2);
                await session.FlushAsync();
            }
            else
            {
                session.Save(album1);
                session.Save(album2);
                session.Flush();
            }
            AssertAlbumBatchDmlRequests(insertSql, false);
        }

        [Fact]
        public async Task HqlUpdate()
        {
            var sql = "update Album set Title=@p0 where Title=@p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(100));
            using var session = _fixture.SessionFactory.OpenSession();
            var hql = "update Album a set a.Title = :newTitle where a.Title = :oldTitle";
            var updateCount = await session
                .CreateQuery(hql)
                .SetParameter("newTitle", "My new title")
                .SetParameter("oldTitle", "My old title")
                .ExecuteUpdateAsync();
            Assert.Equal(100, updateCount);
            var request = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().FirstOrDefault(r => r.Sql.Equals(sql));
            Assert.NotNull(request);
            Assert.Collection(request.Params.Fields,
                param => Assert.Equal("My new title", param.Value.StringValue),
                param => Assert.Equal("My old title", param.Value.StringValue));
        }

        [Fact]
        public async Task LinqUpdate()
        {
            var sql = "update Album set Title=@p0 where Title=@p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(100));
            using var session = _fixture.SessionFactory.OpenSession();
            var updateCount = await session
                .Query<Album>()
                .Where(a => a.Title.Equals("My old title"))
                .UpdateAsync(a => new Album { Title = "My new title" });
            Assert.Equal(100, updateCount);
            var request = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().FirstOrDefault(r => r.Sql.Equals(sql));
            Assert.NotNull(request);
            Assert.Collection(request.Params.Fields,
                param => Assert.Equal("My new title", param.Value.StringValue),
                param => Assert.Equal("My old title", param.Value.StringValue));
        }

        [Fact]
        public async Task HqlInsert()
        {
            var sql =
                "insert into Album ( AlbumId, Title, SingerId ) "
                      + "select singer0_.SingerId as col_0_0_, singer0_.FullName as col_1_0_, singer0_.SingerId as col_2_0_ "
                      + "from Singer singer0_";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(100));
            using var session = _fixture.SessionFactory.OpenSession();
            var updateCount = await session
                .CreateQuery("insert into Album (AlbumId, Title, Singer) select s.SingerId, s.FullName, s from Singer s")
                .ExecuteUpdateAsync();
            Assert.Equal(100, updateCount);
        }

        [Fact]
        public async Task LinqInsert()
        {
            var sql =
                "insert into Album ( AlbumId, Title, SingerId ) "
                + "select singer0_.SingerId as col_0_0_, singer0_.FullName as col_1_0_, singer0_.SingerId as col_2_0_ "
                + "from Singer singer0_";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(100));
            using var session = _fixture.SessionFactory.OpenSession();
            var updateCount = await session
                .Query<Singer>()
                .InsertBuilder()
                .Into<Album>()
                .Value(a => a.AlbumId, s => s.SingerId)
                .Value(a => a.Title, s => s.FullName)
                .Value(a => a.Singer, s => s)
                .InsertAsync();
            Assert.Equal(100, updateCount);
        }

        [CombinatorialData]
        [Theory]
        public async Task InsertUsingMutation(bool async)
        {
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var album = new Album
            {
                AlbumId = 1L,
                Title = "My title"
            };
            if (async)
            {
                await session.SaveAsync(album);
                await session.FlushAsync();
            }
            else
            {
                session.Save(album);
                session.Flush();
            }
            var commit = _fixture.SpannerMock.Requests.OfType<CommitRequest>().FirstOrDefault();
            Assert.NotNull(commit);
            Assert.Collection(commit.Mutations, mutation =>
            {
                Assert.Equal(Mutation.OperationOneofCase.Insert, mutation.OperationCase);
            });
        }

        [CombinatorialData]
        [Theory]
        public async Task InsertsUsingMutations(bool async)
        {
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var album1 = new Album
            {
                AlbumId = 1L,
                Title = "My first title"
            };
            var album2 = new Album
            {
                AlbumId = 2L,
                Title = "My second title"
            };
            if (async)
            {
                await session.SaveAsync(album1);
                await session.SaveAsync(album2);
                await session.FlushAsync();
            }
            else
            {
                session.Save(album1);
                session.Save(album2);
                session.Flush();
            }
            var commit = _fixture.SpannerMock.Requests.OfType<CommitRequest>().FirstOrDefault();
            Assert.NotNull(commit);
            Assert.Collection(commit.Mutations, mutation =>
            {
                Assert.Equal(Mutation.OperationOneofCase.Insert, mutation.OperationCase);
            }, mutation =>
            {
                Assert.Equal(Mutation.OperationOneofCase.Insert, mutation.OperationCase);
            });
        }

        [CombinatorialData]
        [Theory]
        public async Task ExceedBatchSizeWithMoreThanOne(bool async)
        {
            var insertSql = "INSERT INTO Album (SingerId, Title, ReleaseDate, AlbumId) VALUES (@p0, @p1, @p2, @p3)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            var batchSize = ((SessionFactoryImpl)_fixture.SessionFactory).Settings.AdoBatchSize;

            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            // We exceed the batch size with two to ensure we get two BatchDML requests, and not one BatchDML
            // and one ExecuteSql request.
            for (var row = 0; row < batchSize + 2; row++)
            {
                var album = new Album
                {
                    AlbumId = row,
                    Title = $"Title {row}",
                };
                if (async)
                {
                    await session.SaveAsync(album);
                }
                else
                {
                    session.Save(album);
                }
            }
            if (async)
            {
                await transaction.CommitAsync();
            }
            else
            {
                transaction.Commit();
            }

            var batchRequests = _fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>();
            // We should have two batch requests, as we exceed the batch size.
            Assert.Equal(2, batchRequests.Count());
            var commitRequests = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Single(commitRequests);
        }

        [CombinatorialData]
        [Theory]
        public async Task ExceedBatchSizeWithOne(bool async)
        {
            var insertSql = "INSERT INTO Album (SingerId, Title, ReleaseDate, AlbumId) VALUES (@p0, @p1, @p2, @p3)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            var batchSize = ((SessionFactoryImpl)_fixture.SessionFactory).Settings.AdoBatchSize;

            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            // We exceed the batch size with exactly one to ensure we get one BatchDML request and
            // one ExecuteSql request.
            for (var row = 0; row < batchSize + 1; row++)
            {
                var album = new Album
                {
                    AlbumId = row,
                    Title = $"Title {row}",
                };
                if (async)
                {
                    await session.SaveAsync(album);
                }
                else
                {
                    session.Save(album);
                }
            }
            if (async)
            {
                await transaction.CommitAsync();
            }
            else
            {
                transaction.Commit();
            }

            var batchRequests = _fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>();
            Assert.Single(batchRequests);
            var executeRequests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Single(executeRequests);
            var commitRequests = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Single(commitRequests);
        }

        [CombinatorialData]
        [Theory]
        public async Task ExceedBatchSizeUsingMutations(bool async)
        {
            var batchSize = ((SessionFactoryImpl)_fixture.SessionFactoryUsingMutations).Settings.AdoBatchSize;

            using var session = _fixture.SessionFactoryUsingMutations.OpenSession();
            using var transaction = session.BeginTransaction(MutationUsage.Always);
            for (var row = 0; row < batchSize + 1; row++)
            {
                var album = new Album
                {
                    AlbumId = row,
                    Title = $"Title {row}",
                };
                if (async)
                {
                    await session.SaveAsync(album);
                }
                else
                {
                    session.Save(album);
                }
            }
            if (async)
            {
                await transaction.CommitAsync();
            }
            else
            {
                transaction.Commit();
            }

            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            // Even though the mutations are 'flushed' twice to the transaction, there will only be one
            // commit request containing all the mutations.
            var commitRequests = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commitRequests, request =>
            {
                Assert.Equal(batchSize + 1, request.Mutations.Count);
            });
        }
        
        private string QueryAllAlbumsSql() =>
            "select album0_.AlbumId as albumid1_1_, album0_.SingerId as singerid2_1_, album0_.Title as title3_1_, album0_.ReleaseDate as releasedate4_1_ from Album album0_";

        private string AddGetAlbumResult(string sql, Album album) =>
            AddGetAlbumResult(sql, new [] { new object[] { album.AlbumId, album.Singer?.SingerId, album.Title, album.ReleaseDate } });

        private string AddGetAlbumResult(string sql, IEnumerable<object[]> rows)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "albumid1_1_0_"),
                    Tuple.Create(V1.TypeCode.Int64, "singerid2_1_0_"),
                    Tuple.Create(V1.TypeCode.String, "title3_1_0_"),
                    Tuple.Create(V1.TypeCode.Date, "releasedate4_1_0_"),
                }, rows));
            return sql;
        }
        
        private string AddQueryAlbumsResults(string sql, params Album[] albums) =>
            AddQueryAlbumsResult(sql, albums.Select(album => new object[] { album.AlbumId, album.Singer?.SingerId, album.Title, album.ReleaseDate }).ToArray());

        private string AddQueryAlbumsResult(string sql, IEnumerable<object[]> rows)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "albumid1_1_"),
                    Tuple.Create(V1.TypeCode.Int64, "singerid2_1_"),
                    Tuple.Create(V1.TypeCode.String, "title3_1_"),
                    Tuple.Create(V1.TypeCode.Date, "releasedate4_1_"),
                }, rows));
            return sql;
        }
    }
}