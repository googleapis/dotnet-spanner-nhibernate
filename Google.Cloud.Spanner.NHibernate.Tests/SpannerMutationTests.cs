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
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.NHibernate.Tests.Entities;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using NHibernate.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    public class SpannerMutationTests : IClassFixture<NHibernateMockServerFixture>
    {
        private readonly NHibernateMockServerFixture _fixture;

        public SpannerMutationTests(NHibernateMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task InsertUsingMutation(bool async, bool explicitTransaction)
        {
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var album = new Album
            {
                AlbumId = 1L,
                Title = "My title"
            };
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            if (async)
            {
                await session.SaveAsync(album);
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                session.Save(album);
                session.Flush();
                transaction?.Commit();
            }

            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits,
                commit =>
                    Assert.Collection(commit.Mutations, mutation =>
                    {
                        Assert.Equal("Album", mutation.Insert.Table);
                        Assert.Collection(mutation.Insert.Columns,
                            c => Assert.Equal("Title", c),
                            c => Assert.Equal("ReleaseDate", c),
                            c => Assert.Equal("SingerId", c),
                            c => Assert.Equal("AlbumId", c));
                        Assert.Collection(mutation.Insert.Values,
                            row => Assert.Collection(row.Values,
                                value => Assert.Equal("My title", value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                value => Assert.Equal("1", value.StringValue)));
                    }));
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task InsertsUsingMutations(bool async, bool explicitTransaction)
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
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            if (async)
            {
                await session.SaveAsync(album1);
                await session.SaveAsync(album2);
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                session.Save(album1);
                session.Save(album2);
                session.Flush();
                transaction?.Commit();
            }
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits,
                commit =>
                    Assert.Collection(commit.Mutations, mutation =>
                    {
                        Assert.Equal("Album", mutation.Insert.Table);
                        Assert.Collection(mutation.Insert.Columns,
                            c => Assert.Equal("Title", c),
                            c => Assert.Equal("ReleaseDate", c),
                            c => Assert.Equal("SingerId", c),
                            c => Assert.Equal("AlbumId", c));
                        Assert.Collection(mutation.Insert.Values,
                            row => Assert.Collection(row.Values,
                                value => Assert.Equal("My first title", value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                value => Assert.Equal("1", value.StringValue)));
                    }, mutation =>
                    {
                        Assert.Equal("Album", mutation.Insert.Table);
                        Assert.Collection(mutation.Insert.Columns,
                            c => Assert.Equal("Title", c),
                            c => Assert.Equal("ReleaseDate", c),
                            c => Assert.Equal("SingerId", c),
                            c => Assert.Equal("AlbumId", c));
                        Assert.Collection(mutation.Insert.Values,
                            row => Assert.Collection(row.Values,
                                value => Assert.Equal("My second title", value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                value => Assert.Equal("2", value.StringValue)));
                    }));
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task InsertsUsingMutationsAndMultipleFlushes(bool async, bool explicitTransaction)
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
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            if (async)
            {
                await session.SaveAsync(album1);
                await session.FlushAsync();
                await session.SaveAsync(album2);
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                session.Save(album1);
                session.Flush();
                session.Save(album2);
                session.Flush();
                transaction?.Commit();
            }
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            if (explicitTransaction)
            {
                // There is an explicit transaction, so each flush only buffer the mutations on the connection.
                Assert.Collection(commits,
                    commit =>
                        Assert.Collection(commit.Mutations, mutation =>
                        {
                            Assert.Equal("Album", mutation.Insert.Table);
                            Assert.Collection(mutation.Insert.Columns,
                                c => Assert.Equal("Title", c),
                                c => Assert.Equal("ReleaseDate", c),
                                c => Assert.Equal("SingerId", c),
                                c => Assert.Equal("AlbumId", c));
                            Assert.Collection(mutation.Insert.Values,
                                row => Assert.Collection(row.Values,
                                    value => Assert.Equal("My first title", value.StringValue),
                                    value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                    value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                    value => Assert.Equal("1", value.StringValue)));
                        }, mutation =>
                        {
                            Assert.Equal("Album", mutation.Insert.Table);
                            Assert.Collection(mutation.Insert.Columns,
                                c => Assert.Equal("Title", c),
                                c => Assert.Equal("ReleaseDate", c),
                                c => Assert.Equal("SingerId", c),
                                c => Assert.Equal("AlbumId", c));
                            Assert.Collection(mutation.Insert.Values,
                                row => Assert.Collection(row.Values,
                                    value => Assert.Equal("My second title", value.StringValue),
                                    value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                    value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                    value => Assert.Equal("2", value.StringValue)));
                        }));
            }
            else
            {
                // There is no explicit transaction, so each flush will cause a commit.
                Assert.Collection(commits,
                    commit =>
                        Assert.Collection(commit.Mutations, mutation =>
                        {
                            Assert.Equal("Album", mutation.Insert.Table);
                            Assert.Collection(mutation.Insert.Columns,
                                c => Assert.Equal("Title", c),
                                c => Assert.Equal("ReleaseDate", c),
                                c => Assert.Equal("SingerId", c),
                                c => Assert.Equal("AlbumId", c));
                            Assert.Collection(mutation.Insert.Values,
                                row => Assert.Collection(row.Values,
                                    value => Assert.Equal("My first title", value.StringValue),
                                    value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                    value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                    value => Assert.Equal("1", value.StringValue)));
                        }),
                    commit =>
                    {
                        Assert.Collection(commit.Mutations, mutation =>
                        {
                            Assert.Equal("Album", mutation.Insert.Table);
                            Assert.Collection(mutation.Insert.Columns,
                                c => Assert.Equal("Title", c),
                                c => Assert.Equal("ReleaseDate", c),
                                c => Assert.Equal("SingerId", c),
                                c => Assert.Equal("AlbumId", c));
                            Assert.Collection(mutation.Insert.Values,
                                row => Assert.Collection(row.Values,
                                    value => Assert.Equal("My second title", value.StringValue),
                                    value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                    value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                    value => Assert.Equal("2", value.StringValue)));
                        });
                    });
            }
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task UpdateUsingMutation(bool async, bool explicitTransaction)
        {
            AddGetAlbumResult(GetAlbumSql(), new Album { AlbumId = 1L, Title = "My album" });
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            if (async)
            {
                var album = await session.GetAsync<Album>(1L);
                album.Title = "New title";
                await session.UpdateAsync(album);
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                var album = session.Get<Album>(1L);
                album.Title = "New title";
                session.Update(album);
                session.Flush();
                transaction?.Commit();
            }
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits,
                commit =>
                    Assert.Collection(commit.Mutations, mutation =>
                    {
                        Assert.Equal("Album", mutation.Update.Table);
                        Assert.Collection(mutation.Update.Columns,
                            c => Assert.Equal("Title", c),
                            c => Assert.Equal("AlbumId", c));
                        Assert.Collection(mutation.Update.Values,
                            row => Assert.Collection(row.Values,
                                value => Assert.Equal("New title", value.StringValue),
                                value => Assert.Equal("1", value.StringValue)));
                    }));
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task UpdatesUsingMutations(bool async, bool explicitTransaction)
        {
            AddQueryAlbumsResults(QueryAllAlbumsSql(),
                new Album { AlbumId = 1L, Title = "My album 1" },
                new Album { AlbumId = 2L, Title = "My album 2" });
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            if (async)
            {
                var albums = await session.Query<Album>().ToListAsync();
                albums.ForEach(a => a.Title += " - updated");
                foreach (var a in albums)
                {
                    await session.UpdateAsync(a);
                }
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                var albums = session.Query<Album>().ToList();
                albums.ForEach(a => a.Title += " - updated");
                albums.ForEach(a => session.Update(a));
                session.Flush();
                transaction?.Commit();
            }
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits,
                commit =>
                {
                    Assert.Collection(commit.Mutations,
                        mutation =>
                        {
                            Assert.Equal("Album", mutation.Update.Table);
                            Assert.Collection(mutation.Update.Columns,
                                c => Assert.Equal("Title", c),
                                c => Assert.Equal("AlbumId", c));
                            Assert.Collection(mutation.Update.Values,
                                row => Assert.Collection(row.Values,
                                    value => Assert.Equal("My album 1 - updated", value.StringValue),
                                    value => Assert.Equal("1", value.StringValue)));
                        },
                        mutation =>
                        {
                            Assert.Equal("Album", mutation.Update.Table);
                            Assert.Collection(mutation.Update.Columns,
                                c => Assert.Equal("Title", c),
                                c => Assert.Equal("AlbumId", c));
                            Assert.Collection(mutation.Update.Values,
                                row => Assert.Collection(row.Values,
                                    value => Assert.Equal("My album 2 - updated", value.StringValue),
                                    value => Assert.Equal("2", value.StringValue)));
                        });
                });
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task UpdatesUsingMutationsAndMultipleFlushes(bool async, bool explicitTransaction)
        {
            AddQueryAlbumsResults(QueryAllAlbumsSql(),
                new Album { AlbumId = 1L, Title = "My album 1" },
                new Album { AlbumId = 2L, Title = "My album 2" });
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            if (async)
            {
                var albums = await session.Query<Album>().ToListAsync();
                albums[0].Title += " - updated";
                await session.SaveAsync(albums[0]);
                await session.FlushAsync();
                albums[1].Title += " - updated";
                await session.SaveAsync(albums[1]);
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                var albums = session.Query<Album>().ToList();
                albums[0].Title += " - updated";
                session.Save(albums[0]);
                session.Flush();
                albums[1].Title += " - updated";
                session.Save(albums[1]);
                session.Flush();
                transaction?.Commit();
            }
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            if (explicitTransaction)
            {
                Assert.Collection(commits,
                    commit =>
                    {
                        Assert.Collection(commit.Mutations,
                            mutation =>
                            {
                                Assert.Equal("Album", mutation.Update.Table);
                                Assert.Collection(mutation.Update.Columns,
                                    c => Assert.Equal("Title", c),
                                    c => Assert.Equal("AlbumId", c));
                                Assert.Collection(mutation.Update.Values,
                                    row => Assert.Collection(row.Values,
                                        value => Assert.Equal("My album 1 - updated", value.StringValue),
                                        value => Assert.Equal("1", value.StringValue)));
                            },
                            mutation =>
                            {
                                Assert.Equal("Album", mutation.Update.Table);
                                Assert.Collection(mutation.Update.Columns,
                                    c => Assert.Equal("Title", c),
                                    c => Assert.Equal("AlbumId", c));
                                Assert.Collection(mutation.Update.Values,
                                    row => Assert.Collection(row.Values,
                                        value => Assert.Equal("My album 2 - updated", value.StringValue),
                                        value => Assert.Equal("2", value.StringValue)));
                            });
                    });
            }
            else
            {
                // There is no explicit transaction, so each flush will cause a Commit.
                Assert.Collection(commits,
                    commit =>
                    {
                        Assert.Collection(commit.Mutations,
                            mutation =>
                            {
                                Assert.Equal("Album", mutation.Update.Table);
                                Assert.Collection(mutation.Update.Columns,
                                    c => Assert.Equal("Title", c),
                                    c => Assert.Equal("AlbumId", c));
                                Assert.Collection(mutation.Update.Values,
                                    row => Assert.Collection(row.Values,
                                        value => Assert.Equal("My album 1 - updated", value.StringValue),
                                        value => Assert.Equal("1", value.StringValue)));
                            });
                    },
                    commit =>
                    {
                        Assert.Collection(commit.Mutations,
                            mutation =>
                            {
                                Assert.Equal("Album", mutation.Update.Table);
                                Assert.Collection(mutation.Update.Columns,
                                    c => Assert.Equal("Title", c),
                                    c => Assert.Equal("AlbumId", c));
                                Assert.Collection(mutation.Update.Values,
                                    row => Assert.Collection(row.Values,
                                        value => Assert.Equal("My album 2 - updated", value.StringValue),
                                        value => Assert.Equal("2", value.StringValue)));
                            });
                    });
            }
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task DeleteUsingMutation(bool async, bool explicitTransaction)
        {
            AddGetAlbumResult(GetAlbumSql(), new Album { AlbumId = 1L, Title = "My album" });
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            if (async)
            {
                var album = await session.GetAsync<Album>(1L);
                await session.DeleteAsync(album);
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                var album = session.Get<Album>(1L);
                session.Delete(album);
                session.Flush();
                transaction?.Commit();
            }
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits,
                commit =>
                    Assert.Collection(commit.Mutations, mutation =>
                    {
                        Assert.Equal("Album", mutation.Delete.Table);
                        Assert.Collection(mutation.Delete.KeySet.Keys, keyValues => Assert.Collection(keyValues.Values, key => Assert.Equal("1", key.StringValue)));
                    }));
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task DeleteUsingMutations(bool async, bool explicitTransaction)
        {
            AddQueryAlbumsResults(QueryAllAlbumsSql(),
                new Album { AlbumId = 1L, Title = "My album 1" },
                new Album { AlbumId = 2L, Title = "My album 2" });
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            if (async)
            {
                var albums = await session.Query<Album>().ToListAsync();
                foreach (var a in albums)
                {
                    await session.DeleteAsync(a);
                }
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                var albums = session.Query<Album>().ToList();
                albums.ForEach(a => session.Delete(a));
                session.Flush();
                transaction?.Commit();
            }
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits,
                commit =>
                    Assert.Collection(commit.Mutations, mutation =>
                    {
                        Assert.Equal("Album", mutation.Delete.Table);
                        Assert.Collection(mutation.Delete.KeySet.Keys, keyValues => Assert.Collection(keyValues.Values, key => Assert.Equal("1", key.StringValue)));
                    }, mutation =>
                    {
                        Assert.Equal("Album", mutation.Delete.Table);
                        Assert.Collection(mutation.Delete.KeySet.Keys, keyValues => Assert.Collection(keyValues.Values, key => Assert.Equal("2", key.StringValue)));
                    }));
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task DeletesUsingMutationsAndMultipleFlushes(bool async, bool explicitTransaction)
        {
            AddQueryAlbumsResults(QueryAllAlbumsSql(),
                new Album { AlbumId = 1L, Title = "My album 1" },
                new Album { AlbumId = 2L, Title = "My album 2" });
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            if (async)
            {
                var albums = await session.Query<Album>().ToListAsync();
                await session.DeleteAsync(albums[0]);
                await session.FlushAsync();
                await session.DeleteAsync(albums[1]);
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                var albums = session.Query<Album>().ToList();
                session.Delete(albums[0]);
                session.Flush();
                session.Delete(albums[1]);
                session.Flush();
                transaction?.Commit();
            }
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            if (explicitTransaction)
            {
                Assert.Collection(commits,
                    commit =>
                        Assert.Collection(commit.Mutations, mutation =>
                        {
                            Assert.Equal("Album", mutation.Delete.Table);
                            Assert.Collection(mutation.Delete.KeySet.Keys, keyValues => Assert.Collection(keyValues.Values, key => Assert.Equal("1", key.StringValue)));
                        }, mutation =>
                        {
                            Assert.Equal("Album", mutation.Delete.Table);
                            Assert.Collection(mutation.Delete.KeySet.Keys, keyValues => Assert.Collection(keyValues.Values, key => Assert.Equal("2", key.StringValue)));
                        }));
            }
            else
            {
                // There is no explicit transaction, so each flush will cause a Commit.
                Assert.Collection(commits,
                    commit =>
                        Assert.Collection(commit.Mutations, mutation =>
                        {
                            Assert.Equal("Album", mutation.Delete.Table);
                            Assert.Collection(mutation.Delete.KeySet.Keys, keyValues => Assert.Collection(keyValues.Values, key => Assert.Equal("1", key.StringValue)));
                        }),
                    commit =>
                        Assert.Collection(commit.Mutations, mutation =>
                        {
                            Assert.Equal("Album", mutation.Delete.Table);
                            Assert.Collection(mutation.Delete.KeySet.Keys, keyValues => Assert.Collection(keyValues.Values, key => Assert.Equal("2", key.StringValue)));
                        }));
            }
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task InsertWithGeneratedValueUsingMutation(bool async, bool explicitTransaction)
        {
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var singer = new Singer
            {
                SingerId = 1L,
                FirstName = "Alice",
                LastName = "Peterson",
            };
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            if (async)
            {
                await session.SaveAsync(singer);
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                session.Save(singer);
                session.Flush();
                transaction?.Commit();
            }

            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits,
                commit =>
                    Assert.Collection(commit.Mutations, mutation =>
                    {
                        Assert.Equal("Singer", mutation.Insert.Table);
                        Assert.Collection(mutation.Insert.Columns,
                            c => Assert.Equal("FirstName", c),
                            c => Assert.Equal("LastName", c),
                            c => Assert.Equal("BirthDate", c),
                            c => Assert.Equal("Picture", c),
                            c => Assert.Equal("SingerId", c));
                        Assert.Collection(mutation.Insert.Values,
                            row => Assert.Collection(row.Values,
                                value => Assert.Equal("Alice", value.StringValue),
                                value => Assert.Equal("Peterson", value.StringValue),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                value => Assert.Equal(Value.KindOneofCase.NullValue, value.KindCase),
                                value => Assert.Equal("1", value.StringValue)));
                    }));
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task InsertWithCommitTimestampUsingMutation(bool async, bool explicitTransaction)
        {
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var entity = new TableWithAllColumnTypes
            {
                ColInt64 = 1L,
            };
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            if (async)
            {
                await session.SaveAsync(entity);
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                session.Save(entity);
                session.Flush();
                transaction?.Commit();
            }

            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits,
                commit =>
                    Assert.Collection(commit.Mutations, mutation =>
                    {
                        Assert.Equal("TableWithAllColumnTypes", mutation.Insert.Table);
                        Assert.Collection(mutation.Insert.Columns,
                            c => Assert.Equal("ColFloat64", c),
                            c => Assert.Equal("ColNumeric", c),
                            c => Assert.Equal("ColBool", c),
                            c => Assert.Equal("ColString", c),
                            c => Assert.Equal("ColStringMax", c),
                            c => Assert.Equal("ColBytes", c),
                            c => Assert.Equal("ColBytesMax", c),
                            c => Assert.Equal("ColDate", c),
                            c => Assert.Equal("ColTimestamp", c),
                            c => Assert.Equal("ColJson", c),
                            c => Assert.Equal("ColInt64Array", c),
                            c => Assert.Equal("ColFloat64Array", c),
                            c => Assert.Equal("ColNumericArray", c),
                            c => Assert.Equal("ColBoolArray", c),
                            c => Assert.Equal("ColStringArray", c),
                            c => Assert.Equal("ColStringMaxArray", c),
                            c => Assert.Equal("ColBytesArray", c),
                            c => Assert.Equal("ColBytesMaxArray", c),
                            c => Assert.Equal("ColDateArray", c),
                            c => Assert.Equal("ColTimestampArray", c),
                            c => Assert.Equal("ColJsonArray", c),
                            c => Assert.Equal("ASC", c),
                            c => Assert.Equal("ColInt64", c),
                            c => Assert.Equal("ColCommitTs", c));
                        Assert.Collection(mutation.Insert.Values,
                            row => Assert.Collection(row.Values,
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal(Value.KindOneofCase.NullValue, column.KindCase),
                                column => Assert.Equal("1", column.StringValue),
                                column => Assert.Equal("spanner.commit_timestamp()", column.StringValue)));
                    }));
        }

        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        [Theory]
        public async Task UpdateWithCommitTimestampUsingMutation(bool async, bool explicitTransaction)
        {
            var sql = "/* load Google.Cloud.Spanner.NHibernate.Tests.Entities.TableWithAllColumnTypes */ SELECT tablewitha0_.ColInt64 as colint1_2_0_, tablewitha0_.ColFloat64 as colfloat2_2_0_, tablewitha0_.ColNumeric as colnumeric3_2_0_, tablewitha0_.ColBool as colbool4_2_0_, tablewitha0_.ColString as colstring5_2_0_, tablewitha0_.ColStringMax as colstringmax6_2_0_, tablewitha0_.ColBytes as colbytes7_2_0_, tablewitha0_.ColBytesMax as colbytesmax8_2_0_, tablewitha0_.ColDate as coldate9_2_0_, tablewitha0_.ColTimestamp as coltimestamp10_2_0_, tablewitha0_.ColJson as coljson11_2_0_, tablewitha0_.ColCommitTs as colcommitts12_2_0_, tablewitha0_.ColInt64Array as colint13_2_0_, tablewitha0_.ColFloat64Array as colfloat14_2_0_, tablewitha0_.ColNumericArray as colnumericarray15_2_0_, tablewitha0_.ColBoolArray as colboolarray16_2_0_, tablewitha0_.ColStringArray as colstringarray17_2_0_, tablewitha0_.ColStringMaxArray as colstringmaxarray18_2_0_, tablewitha0_.ColBytesArray as colbytesarray19_2_0_, tablewitha0_.ColBytesMaxArray as colbytesmaxarray20_2_0_, tablewitha0_.ColDateArray as coldatearray21_2_0_, tablewitha0_.ColTimestampArray as coltimestamparray22_2_0_, tablewitha0_.ColJsonArray as coljsonarray23_2_0_, tablewitha0_.ColComputed as colcomputed24_2_0_, tablewitha0_.ASC as asc25_2_0_ FROM TableWithAllColumnTypes tablewitha0_ WHERE tablewitha0_.ColInt64=@p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, SpannerDriverTest.CreateTableWithAllColumnTypesResultSet(SpannerDriverTest.CreateRowWithAllColumnTypes()));
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.ImplicitTransactions);
            var transaction = explicitTransaction ? session.BeginTransaction(MutationUsage.Always) : null;
            TableWithAllColumnTypes entity;
            if (async)
            {
                entity = await session.LoadAsync<TableWithAllColumnTypes>(1L);
                entity.ColBool = !entity.ColBool;
                await session.UpdateAsync(entity);
                await session.FlushAsync();
                await (transaction?.CommitAsync() ?? Task.CompletedTask);
            }
            else
            {
                entity = session.Load<TableWithAllColumnTypes>(1L);
                entity.ColBool = !entity.ColBool;
                session.Update(entity);
                session.Flush();
                transaction?.Commit();
            }

            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits,
                commit =>
                    Assert.Collection(commit.Mutations, mutation =>
                    {
                        Assert.Equal("TableWithAllColumnTypes", mutation.Update.Table);
                        Assert.Collection(mutation.Update.Columns,
                            c => Assert.Equal("ColBool", c),
                            c => Assert.Equal("ColInt64", c),
                            c => Assert.Equal("ColCommitTs", c));
                        Assert.Collection(mutation.Update.Values,
                            row => Assert.Collection(row.Values,
                                column => Assert.Equal(entity.ColBool, column.BoolValue),
                                column => Assert.Equal("1", column.StringValue),
                                column => Assert.Equal("spanner.commit_timestamp()", column.StringValue)));
                    }));
        }

        private string GetAlbumSql() =>
            "/* load Google.Cloud.Spanner.NHibernate.Tests.Entities.Album */ SELECT album0_.AlbumId as albumid1_1_0_, album0_.Title as title2_1_0_, album0_.ReleaseDate as releasedate3_1_0_, album0_.SingerId as singerid4_1_0_ FROM Album album0_ WHERE album0_.AlbumId=@p0";
        
        private string QueryAllAlbumsSql() =>
            "/* [expression] */select album0_.AlbumId as albumid1_1_, album0_.Title as title2_1_, album0_.ReleaseDate as releasedate3_1_, album0_.SingerId as singerid4_1_ from Album album0_";

        private string AddGetAlbumResult(string sql, Album album) =>
            AddGetAlbumResult(sql, new [] { new object[] { album.AlbumId, album.Title, album.ReleaseDate, album.Singer?.SingerId } });

        private string AddGetAlbumResult(string sql, IEnumerable<object[]> rows)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "albumid1_1_0_"),
                    Tuple.Create(V1.TypeCode.String, "title2_1_0_"),
                    Tuple.Create(V1.TypeCode.Date, "releasedate3_1_0_"),
                    Tuple.Create(V1.TypeCode.Int64, "singerid4_1_0_"),
                }, rows));
            return sql;
        }
        
        private string AddQueryAlbumsResults(string sql, params Album[] albums) =>
            AddQueryAlbumsResult(sql, albums.Select(album => new object[] { album.AlbumId, album.Title, album.ReleaseDate, album.Singer?.SingerId }).ToArray());

        private string AddQueryAlbumsResult(string sql, IEnumerable<object[]> rows)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "albumid1_1_"),
                    Tuple.Create(V1.TypeCode.String, "title2_1_"),
                    Tuple.Create(V1.TypeCode.Date, "releasedate3_1_"),
                    Tuple.Create(V1.TypeCode.Int64, "singerid4_1_"),
                }, rows));
            return sql;
        }
    }
}