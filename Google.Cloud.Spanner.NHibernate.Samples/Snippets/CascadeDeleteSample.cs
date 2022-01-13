// Copyright 2022 Google Inc. All Rights Reserved.
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
using NHibernate.Linq;
using System;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// This sample shows how to work with the ON DELETE CASCADE option for interleaved tables in NHibernate.
    /// An interleaved table is a child table of another table. Rows of the child table are stored physically together
    /// with the associated rows of the parent table. This sample defines the Tracks table as an interleaved table of
    /// the Albums table with the ON DELETE CASCADE option enabled. This means that deleting an Album will automatically
    /// also delete all Track records of the Album.
    ///
    /// See <see cref="AlbumMapping"/> for an example of how the ON DELETE CASCADE option is enabled for a collection.
    /// See <see cref="TrackMapping"/> for an example of how the many-to-one relationship is defined as an INTERLEAVE IN
    /// PARENT relationship instead of a normal FOREIGN KEY constraint.
    ///
    /// NOTE: This example uses the Cloud Spanner ON DELETE CASCADE option for interleaved tables. This is not the same
    /// as the cascade options that can be defined for collection mappings in NHibernate. Those options will cause
    /// NHibernate to execute additional UPDATE/DELETE statements to delete or update child records.
    ///
    /// This sample shows how to use an interleaved table that has already been created.
    /// See <see cref="SchemaExportSample"/> for an example for how to generate a schema from an NHibernate mapping that
    /// includes interleaved tables.
    /// 
    /// See https://cloud.google.com/spanner/docs/schema-and-data-model#creating-interleaved-tables for more information
    /// on interleaved tables in Cloud Spanner.
    /// 
    /// Run from the command line with `dotnet run CascadeDelete`
    /// </summary>
    public static class CascadeDeleteSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            object albumId;
            using var session = configuration.SessionFactory.OpenSession();
            using (var transaction = session.BeginTransaction())
            {
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
                albumId = await session.SaveAsync(album);
                await session.SaveAsync(track);
                await transaction.CommitAsync();
            }

            // Verify that all albums and tracks have been deleted.
            var albums = await session
                .Query<Album>()
                .ToListAsync();
            var tracks = await session
                .Query<Track>()
                .ToListAsync();
            Console.WriteLine($"Number of albums BEFORE delete: {albums.Count}");
            Console.WriteLine($"Number of tracks BEFORE delete: {tracks.Count}");
            Console.WriteLine();

            // Now delete the Album without deleting the Track first. This will succeed, as Cloud Spanner will also
            // delete any Track records that are stored together with the Album record.
            // See the collection mapping of Tracks in the AlbumMapping class
            using (var transaction = session.BeginTransaction())
            {
                var album = await session.LoadAsync<Album>(albumId);
                await session.DeleteAsync(album);
                await transaction.CommitAsync();
            }

            // Verify that all albums and tracks have been deleted.
            albums = await session
                .Query<Album>()
                .ToListAsync();
            tracks = await session
                .Query<Track>()
                .ToListAsync();
            
            Console.WriteLine($"Number of albums AFTER delete: {albums.Count}");
            Console.WriteLine($"Number of tracks AFTER delete: {tracks.Count}");
            Console.WriteLine();
        }
    }
}