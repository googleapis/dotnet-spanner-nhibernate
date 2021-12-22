// Copyright 2021 Google Inc. All Rights Reserved.
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

using System.Threading.Tasks;
using Google.Cloud.Spanner.NHibernate.IntegrationTests.SampleEntities;
using NHibernate;
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests
{
    public class VersioningTests : IClassFixture<SpannerSampleFixture>
    {
        private readonly SpannerSampleFixture _fixture;

        public VersioningTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [InlineData(MutationUsage.Always)]
        [InlineData(MutationUsage.Never)]
        [Theory]
        public async Task InitialVersionIsGeneratedAtInsert(MutationUsage mutationUsage)
        {
            var sessionFactory = mutationUsage == MutationUsage.Always
                ? _fixture.SessionFactoryForMutations
                : _fixture.SessionFactory;
            string singerId;
            using (var session = sessionFactory.OpenSession())
            {
                var singer = new SingerWithVersion
                {
                    FirstName = "Pete",
                    LastName = "Allison"
                };

                var album = new AlbumWithVersion
                {
                    Title = "A new title",
                    Singer = singer
                };

                using (var transaction = session.BeginTransaction(mutationUsage))
                {
                    singerId = (string)await session.SaveAsync(singer);
                    await session.SaveAsync(album);
                    await transaction.CommitAsync();
                }
            }

            using (var session = sessionFactory.OpenSession())
            {
                var singer = await session.LoadAsync<SingerWithVersion>(singerId);
                Assert.Equal(1, singer.Version);
                Assert.Collection(singer.Albums, a => Assert.Equal(1, a.Version));
            }
        }

        [InlineData(MutationUsage.Always)]
        [InlineData(MutationUsage.Never)]
        [Theory]
        public async Task VersionIsUpdated(MutationUsage mutationUsage)
        {
            var sessionFactory = mutationUsage == MutationUsage.Always
                ? _fixture.SessionFactoryForMutations
                : _fixture.SessionFactory;
            string singerId;
            using (var session = sessionFactory.OpenSession())
            {
                var singer = new SingerWithVersion
                {
                    FirstName = "Pete",
                    LastName = "Allison"
                };

                var album = new AlbumWithVersion
                {
                    Title = "A new title",
                    Singer = singer
                };

                using (var transaction = session.BeginTransaction(mutationUsage))
                {
                    singerId = (string) await session.SaveAsync(singer);
                    await session.SaveAsync(album);
                    await transaction.CommitAsync();
                }

                using (var transaction = session.BeginTransaction(mutationUsage))
                {
                    // Update both the singer and album.
                    singer.FirstName = "Zeke";
                    album.Title = "Other title";
                    await session.SaveAsync(singer);
                    await session.SaveAsync(album);
                    await transaction.CommitAsync();
                }
            }

            using (var session = sessionFactory.OpenSession())
            {
                var singer = await session.LoadAsync<SingerWithVersion>(singerId);
                Assert.Equal(2, singer.Version);
                Assert.Collection(singer.Albums, a => Assert.Equal(2, a.Version));
            }
        }

        [InlineData(MutationUsage.Always)]
        [InlineData(MutationUsage.Never)]
        [Theory]
        public async Task UpdateIsRejectedIfConcurrentUpdateIsDetected(MutationUsage mutationUsage)
        {
            var sessionFactory = mutationUsage == MutationUsage.Always
                ? _fixture.SessionFactoryForMutations
                : _fixture.SessionFactory;
            string singerId;
            string albumId;
            var singer = new SingerWithVersion
            {
                FirstName = "Pete",
                LastName = "Allison"
            };

            var album = new AlbumWithVersion
            {
                Title = "A new title",
                Singer = singer
            };

            using (var session = sessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction(mutationUsage))
                {
                    singerId = (string)await session.SaveAsync(singer);
                    albumId = (string)await session.SaveAsync(album);
                    await transaction.CommitAsync();
                }
            }

            // Update the version number of the records manually to simulate a concurrent update.
            using (var session = sessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    var singersQuery = session.CreateSQLQuery($"UPDATE SingersWithVersion SET Version=2 WHERE Id=\"{singerId}\"");
                    Assert.Equal(1, await singersQuery.ExecuteUpdateAsync());

                    var albumsQuery = session.CreateSQLQuery($"UPDATE AlbumsWithVersion SET Version=2 WHERE Id=\"{albumId}\"");
                    Assert.Equal(1, await albumsQuery.ExecuteUpdateAsync());
                    await transaction.CommitAsync();
                }
            }

            // Try to update the singer and album through NHibernate. That should now fail.
            singer.FirstName = "Zeke";
            album.Title = "Other title";

            using (var session = sessionFactory.OpenSession().SetBatchMutationUsage(mutationUsage))
            {
                await session.SaveOrUpdateAsync(singer);
                await Assert.ThrowsAsync<StaleStateException>(() => session.FlushAsync());
            }

            using (var session = sessionFactory.OpenSession().SetBatchMutationUsage(mutationUsage))
            {
                await session.SaveOrUpdateAsync(album);
                await Assert.ThrowsAsync<StaleStateException>(() => session.FlushAsync());
            }
        }

        [InlineData(MutationUsage.Always)]
        [InlineData(MutationUsage.Never)]
        [Theory]
        public async Task DeleteIsRejectedIfConcurrentUpdateIsDetected(MutationUsage mutationUsage)
        {
            var sessionFactory = mutationUsage == MutationUsage.Always
                ? _fixture.SessionFactoryForMutations
                : _fixture.SessionFactory;
            string singerId;
            string albumId;
            var singer = new SingerWithVersion
            {
                FirstName = "Pete",
                LastName = "Allison"
            };

            var album = new AlbumWithVersion
            {
                Title = "A new title",
                Singer = singer
            };

            using (var session = sessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction(mutationUsage))
                {
                    singerId = (string) await session.SaveAsync(singer);
                    albumId = (string) await session.SaveAsync(album);
                    await transaction.CommitAsync();
                    Assert.Equal(2, session.Statistics.EntityCount);
                }
            }

            // Update the version number of the records manually to simulate a concurrent update.
            using (var session = sessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    var singersQuery = session.CreateSQLQuery($"UPDATE SingersWithVersion SET Version=2 WHERE Id=\"{singerId}\"");
                    Assert.Equal(1, await singersQuery.ExecuteUpdateAsync());

                    var albumsQuery = session.CreateSQLQuery($"UPDATE AlbumsWithVersion SET Version=2 WHERE Id=\"{albumId}\"");
                    Assert.Equal(1, await albumsQuery.ExecuteUpdateAsync());
                    await transaction.CommitAsync();
                }
            }

            // Try to delete the singer. This will fail as the version number no longer matches.
            using (var session = _fixture.SessionFactory.OpenSession().SetBatchMutationUsage(mutationUsage))
            {
                await session.DeleteAsync(album);
                await session.DeleteAsync(singer);
                await Assert.ThrowsAsync<StaleStateException>(() => session.FlushAsync());
            }
        }
    }
}
