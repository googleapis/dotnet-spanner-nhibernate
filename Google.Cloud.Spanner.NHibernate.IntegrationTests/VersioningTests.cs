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

using System;
using System.Collections.Generic;
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

        [Fact]
        public async Task InitialVersionIsGeneratedAtInsert()
        {
            string singerId;
            using (var session = _fixture.SessionFactory.OpenSession())
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

                using (var transaction = session.BeginTransaction())
                {
                    singerId = (string)await session.SaveAsync(singer);
                    await session.SaveAsync(album);
                    await transaction.CommitAsync();
                }
            }

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                var singer = await session.LoadAsync<SingerWithVersion>(singerId);
                Assert.Equal(1, singer.Version);
                Assert.Collection(singer.Albums, a => Assert.Equal(1, a.Version));
            }
        }

        [Fact]
        public async Task VersionIsUpdated()
        {
            string singerId;
            using (var session = _fixture.SessionFactory.OpenSession())
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

                using (var transaction = session.BeginTransaction())
                {
                    await session.SaveAsync(singer);
                    await session.SaveAsync(album);
                    await transaction.CommitAsync();
                }

                using (var transaction = session.BeginTransaction())
                {
                    // Update both the singer and album.
                    singer.FirstName = "Zeke";
                    album.Title = "Other title";
                    singerId = (string)await session.SaveAsync(singer);
                    await session.SaveAsync(album);
                    await transaction.CommitAsync();
                }
            }

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                var singer = await session.LoadAsync<SingerWithVersion>(singerId);
                Assert.Equal(2, singer.Version);
                Assert.Collection(singer.Albums, a => Assert.Equal(2, a.Version));
            }
        }

        [Fact]
        public async Task UpdateIsRejectedIfConcurrentUpdateIsDetected()
        {
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

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    singerId = (string)await session.SaveAsync(singer);
                    albumId = (string)await session.SaveAsync(album);
                    await transaction.CommitAsync();
                }
            }

            // Update the version number of the records manually to simulate a concurrent update.
            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    var singersQuery = session.CreateSQLQuery($"UPDATE SingersWithVersion SET Version=2 WHERE Id=\"{singerId}\"");
                    Assert.Equal(1, singersQuery.ExecuteUpdate());

                    var albumsQuery = session.CreateSQLQuery($"UPDATE AlbumsWithVersion SET Version=2 WHERE Id=\"{albumId}\"");
                    Assert.Equal(1, albumsQuery.ExecuteUpdate());
                    await transaction.CommitAsync();
                }
            }

            // Try to update the singer and album through EF Core. That should now fail.
            singer.FirstName = "Zeke";
            album.Title = "Other title";

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                session.SaveOrUpdate(singer);
                await Assert.ThrowsAsync<StaleObjectStateException>(() => session.FlushAsync());
            }

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                session.SaveOrUpdate(album);
                await Assert.ThrowsAsync<StaleObjectStateException>(() => session.FlushAsync());
            }
        }

        [Fact]
        public async Task CanDeleteWithCascade()
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

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    await session.SaveAsync(singer);
                    album.Singer = singer;
                    await session.SaveAsync(album);
                    await transaction.CommitAsync();
                    Assert.Equal(2, session.Statistics.EntityCount);
                }
            }

            // Now delete the singer record. This will also cascade delete the album.
            // The total record count will therefore be 2.
            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    await session.DeleteAsync(singer);
                    await transaction.CommitAsync();
                    Assert.Equal(2, session.Statistics.EntityCount);
                }
            }
        }

        [Fact]
        public async Task DeleteIsRejectedIfConcurrentUpdateIsDetected()
        {
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

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    singerId = (string)await session.SaveAsync(singer);
                    albumId = (string)await session.SaveAsync(album);
                    await transaction.CommitAsync();
                    Assert.Equal(2, session.Statistics.EntityCount);
                }
            }

            // Update the version number of the records manually to simulate a concurrent update.
            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using (var transaction = session.BeginTransaction())
                {
                    var singersQuery = session.CreateSQLQuery($"UPDATE SingersWithVersion SET Version=2 WHERE Id=\"{singerId}\"");
                    Assert.Equal(1, singersQuery.ExecuteUpdate());

                    var albumsQuery = session.CreateSQLQuery($"UPDATE AlbumsWithVersion SET Version=2 WHERE Id=\"{albumId}\"");
                    Assert.Equal(1, albumsQuery.ExecuteUpdate());
                    await transaction.CommitAsync();
                }
            }

            // Try to delete the singer. This will fail as the version number no longer matches.
            using (var session = _fixture.SessionFactory.OpenSession())
            {
                await session.DeleteAsync(singer);
                await Assert.ThrowsAsync<StaleObjectStateException>(() => session.FlushAsync());
            }
        }
    }
}
