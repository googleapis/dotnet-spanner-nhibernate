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

using Google.Cloud.Spanner.NHibernate.Samples.SampleModel;
using System;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// This sample shows how to work with interleaved table hierarchies in NHibernate.
    /// An interleaved table is a child table of another table. Rows of the child table are stored physically together
    /// with the associated rows of the parent table. This sample defines the Tracks table as an interleaved table of
    /// the Albums table. The primary key of the Tracks table consists of the primary key of the parent table and a
    /// TrackNumber field.
    /// See <see cref="TrackIdentifier"/> for the definition of the composite primary key for the Tracks table.
    /// See <see cref="Track"/> for the entity definition for the Tracks table.
    /// See <see cref="TrackMapping"/> for the mapping of the Track entity to the Tracks table.
    /// 
    /// See https://cloud.google.com/spanner/docs/schema-and-data-model#creating-interleaved-tables for more information
    /// on interleaved tables in Cloud Spanner.
    /// 
    /// Run from the command line with `dotnet run InterleavedTable`
    /// </summary>
    public static class InterleavedTableSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();

            // Create a Singer, Album and Track.
            var singer = new Singer
            {
                FirstName = "Farhan",
                LastName = "Edwards",
            };
            var album = new Album
            {
                Title = "Thinking Jam",
                Singer = singer,
            };
            var track = new Track
            {
                // A TrackIdentifier consists of the parent Album and a track number.
                TrackIdentifier = new TrackIdentifier(album, 1L),
                Title = "Quit Annually",
            };
            await session.SaveAsync(singer);
            await session.SaveAsync(album);
            await session.SaveAsync(track);
            await transaction.CommitAsync();
            
            // Clear the session to make sure that all entities are loaded from the database and not the first-level
            // cache.
            session.Clear();

            // The primary key of a Track is a TrackIdentifier, which is a combination of an Album and a TrackNumber.
            // We must use this to load a single track.
            var reloadedTrack = await session.LoadAsync<Track>(new TrackIdentifier(album, 1L));
            
            // We can traverse the Track => Album relationship as any other ManyToOne relationship.
            Console.WriteLine($"Track {reloadedTrack.Title} has track number {reloadedTrack.TrackNumber} " +
                              $"on album {reloadedTrack.Album.Title}");

            // Insert a couple of extra tracks.
            await InsertAdditionalTracks(configuration, album);
            // The Album => Track relationship can also be traversed as any other OneToMany relationship.
            var reloadedAlbum = await session.LoadAsync<Album>(album.Id);
            Console.WriteLine($"Tracks on {reloadedAlbum.Title}:");
            foreach (var t in reloadedAlbum.Tracks)
            {
                Console.WriteLine($"\t{t.TrackNumber}: {t.Title}");
            }
        }

        private static async Task InsertAdditionalTracks(SampleConfiguration configuration, Album album)
        {
            using var session = configuration.SessionFactory.OpenSession();
            // NOTE: This method does not use a transaction to create the tracks, and flushes the session after each
            // track that is created. That is because of a limitation in the Spanner Emulator.
            // See https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/25
            await session.SaveAsync(new Track {TrackIdentifier = new TrackIdentifier(album, 2L), Title = "Belong Jointly"});
            await session.FlushAsync();
            await session.SaveAsync(new Track {TrackIdentifier = new TrackIdentifier(album, 3L), Title = "Nurse Straight"});
            await session.FlushAsync();
            await session.SaveAsync(new Track {TrackIdentifier = new TrackIdentifier(album, 4L), Title = "Stop Nevertheless"});
            await session.FlushAsync();
            await session.SaveAsync(new Track {TrackIdentifier = new TrackIdentifier(album, 5L), Title = "Review Aside"});
            await session.FlushAsync();
        }
    }
}