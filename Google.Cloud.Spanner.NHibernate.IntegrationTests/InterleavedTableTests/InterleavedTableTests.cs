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

using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.NHibernate.IntegrationTests.InterleavedTableTests.Entities;
using System;
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests.InterleavedTableTests
{
    public class InterleavedTableTests : IClassFixture<SpannerInterleavedTableFixture>
    {
        private readonly SpannerInterleavedTableFixture _fixture;

        public InterleavedTableTests(SpannerInterleavedTableFixture fixture) => _fixture = fixture;

        [Fact]
        public void CanCreateRows()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            var singer = new Singer
            {
                FirstName = "Pete",
                LastName = "Allison",
            };
            var singerId = session.Save(singer);
            var album = new Album
            {
                AlbumIdentifier = new AlbumIdentifier(singer),
                Title = "My first album"
            };
            var albumId =  session.Save(album);
            var track = new Track
            {
                TrackIdentifier = new TrackIdentifier(album),
                Title = "My first track",
            };
            var trackId = session.Save(track);
            session.Flush();
            session.Clear();

            singer = session.Load<Singer>(singerId);
            album = session.Load<Album>(albumId);
            track = session.Load<Track>(trackId);
            
            Assert.Equal(singerId, singer.SingerId);
            Assert.Equal("Pete", singer.FirstName);
            Assert.Equal("Allison", singer.LastName);
            Assert.Equal("My first album", album.Title);
            
            Assert.Equal(albumId, album.AlbumIdentifier);
            Assert.Equal("My first album", album.Title);
            
            Assert.Equal(trackId, track.TrackIdentifier);
            Assert.Equal("My first track", track.Title);
            
            Assert.Equal(singer.SingerId, album.Singer.SingerId);
            Assert.Equal(singer.FirstName, album.Singer.FirstName);
            Assert.Equal(singer.LastName, album.Singer.LastName);
            
            Assert.Equal(album.AlbumIdentifier, track.Album.AlbumIdentifier);
            Assert.Equal(singer.SingerId, track.Album.Singer.SingerId);
            Assert.Equal(singer.FirstName, track.Album.Singer.FirstName);
            Assert.Equal(singer.LastName, track.Album.Singer.LastName);
            
            Assert.Collection(singer.Albums, album => Assert.Equal("My first album", album.Title));
            Assert.Collection(album.Tracks, track => Assert.Equal("My first track", track.Title));
            Assert.Collection(singer.Tracks, track => Assert.Equal("My first track", track.Title));
            
            transaction.Commit();
        }

        [Fact]
        public void CanUpdateRows()
        {
            object singerId, albumId, trackId;
            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using var transaction = session.BeginTransaction();
                var singer = new Singer
                {
                    FirstName = "Pete",
                    LastName = "Allison",
                };
                singerId = session.Save(singer);
                var album = new Album
                {
                    AlbumIdentifier = new AlbumIdentifier(singer),
                    Title = "My first album"
                };
                albumId = session.Save(album);
                trackId = session.Save(new Track
                {
                    TrackIdentifier = new TrackIdentifier(album),
                    Title = "My first track",
                });
                transaction.Commit();
            }

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using var transaction = session.BeginTransaction();
                var singer = session.Load<Singer>(singerId);
                var album = session.Load<Album>(albumId);
                var track = session.Load<Track>(trackId);

                singer.LastName = "Allison-Peterson";
                album.Title = "My second album";
                track.Title = "My second track";
                transaction.Commit();
            }
            
            using (var session = _fixture.SessionFactory.OpenSession())
            {
                var singer = session.Load<Singer>(singerId);
                var album = session.Load<Album>(albumId);
                var track = session.Load<Track>(trackId);
                Assert.Equal("Pete", singer.FirstName);
                Assert.Equal("Allison-Peterson", singer.LastName);
                Assert.Equal("My second album", album.Title);
                Assert.Equal("My second track", track.Title);
            }
        }

        [Fact]
        public void CanDeleteRows()
        {
            object singerId, albumId, trackId;
            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using var transaction = session.BeginTransaction();
                var singer = new Singer
                {
                    FirstName = "Pete",
                    LastName = "Allison",
                };
                singerId = session.Save(singer);
                var album = new Album
                {
                    AlbumIdentifier = new AlbumIdentifier(singer),
                    Title = "My first album"
                };
                albumId = session.Save(album);
                trackId = session.Save(new Track
                {
                    TrackIdentifier = new TrackIdentifier(album),
                    Title = "My first track",
                });
                transaction.Commit();
            }

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                using var transaction = session.BeginTransaction();
                var singer = session.Load<Singer>(singerId);
                var album = session.Load<Album>(albumId);
                var track = session.Load<Track>(trackId);

                session.Delete(track);
                session.Delete(album);
                session.Delete(singer);
                transaction.Commit();
            }
            
            using (var session = _fixture.SessionFactory.OpenSession())
            {
                var singer = session.Get<Singer>(singerId);
                var album = session.Get<Album>(albumId);
                var track = session.Get<Track>(trackId);
                Assert.Null(singer);
                Assert.Null(album);
                Assert.Null(track);
            }
        }
    }
}
