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
using NHibernate.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// Cloud Spanner supports storing ARRAYs of each of its base types.
    /// The Spanner NHibernate driver includes a user type for each of the supported ARRAY types:
    /// <list type="bullet">
    /// <item><see cref="SpannerBoolArray"/></item>
    /// <item><see cref="SpannerInt64Array"/></item>
    /// <item><see cref="SpannerFloat64Array"/></item>
    /// <item><see cref="SpannerNumericArray"/></item>
    /// <item><see cref="SpannerStringArray"/></item>
    /// <item><see cref="SpannerBytesArray"/></item>
    /// <item><see cref="SpannerDateArray"/></item>
    /// <item><see cref="SpannerTimestampArray"/></item>
    /// <item><see cref="SpannerJsonArray"/></item>
    /// </list>
    /// 
    /// Run from the command line with `dotnet run ArraysSample`
    /// </summary>
    public static class ArraysSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            var (_, album) = await GetSingerAndAlbumAsync(configuration);

            using var session = configuration.SessionFactory.OpenSession();
            // A track has two array columns: Lyrics and LyricsLanguages. The length of both arrays
            // should be equal, as the LyricsLanguages indicate the language of the corresponding Lyrics.
            await session.SaveAsync(new Track
            {
                TrackIdentifier = new TrackIdentifier(album, 1L),
                Title = "Whenever",
                Lyrics = new SpannerStringArray(new List<string> { "Lyrics 1", "Lyrics 2" }),
                LyricsLanguages = new SpannerStringArray(new List<string> { "EN", "DE" }),
            });
            await session.FlushAsync();
            await session.SaveAsync(new Track
            {
                TrackIdentifier = new TrackIdentifier(album, 2L),
                Title = "Wherever",
                // Array elements may be null, regardless whether the column itself is defined as NULL/NOT NULL.
                Lyrics = new SpannerStringArray(new List<string> { null, "Lyrics 2" }),
                LyricsLanguages = new SpannerStringArray(new List<string> { "EN", "DE" }),
            });
            await session.FlushAsync();
            await session.SaveAsync(new Track
            {
                TrackIdentifier = new TrackIdentifier(album, 3L),
                Title = "Probably",
                // ARRAY columns may also be null.
                Lyrics = null,
                LyricsLanguages = null,
            });
            await session.FlushAsync();

            var tracks = await session
                .Query<Track>()
                .OrderBy(t => t.Title)
                .ToListAsync();
            Console.WriteLine("Found tracks:");
            foreach (var track in tracks)
            {
                Console.WriteLine($"Track {track.Title} has lyrics " +
                                  $"{track.Lyrics?.ToString() ?? "NULL"} " +
                                  $"and lyrics languages " +
                                  $"{track.LyricsLanguages?.ToString() ?? "NULL"}");
            }
        }
        
        private static async Task<(Singer, Album)> GetSingerAndAlbumAsync(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            var singer = new Singer
            {
                FirstName = "Hannah",
                LastName = "Polansky"
            };
            await session.SaveAsync(singer);
            var album = new Album
            {
                Singer = singer,
                Title = "Somewhere",
            };
            await session.SaveAsync(album);
            await transaction.CommitAsync();

            return (singer, album);
        }
    }
}