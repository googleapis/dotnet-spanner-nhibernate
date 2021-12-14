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
using Google.Cloud.Spanner.V1;
using NHibernate;
using System;
using System.Collections.Generic;
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
    /// Run from the command line with `dotnet run ArraysSample`    /// </summary>
    public static class ArraysSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            try
            {
                using var session = configuration.SessionFactory.OpenSession();
                using var transaction = session.BeginTransaction();
                var (_, album) = await GetSingerAndAlbumAsync(session);

                // A track has two array columns: Lyrics and LyricsLanguages. The length of both arrays
                // should be equal, as the LyricsLanguages indicate the language of the corresponding Lyrics.
                var track1 = new Track
                {
                    Title = "Whenever",
                    Lyrics = new SpannerStringArray(new List<string> { "Lyrics 1", "Lyrics 2" }),
                    LyricsLanguages = new SpannerStringArray(new List<string> { "EN", "DE" }),
                    Album = album,
                };
                var track2 = new Track
                {
                    Title = "Wherever",
                    // Array elements may be null, regardless whether the column itself is defined as NULL/NOT NULL.
                    Lyrics = new SpannerStringArray(new List<string> { null, "Lyrics 2" }),
                    LyricsLanguages = new SpannerStringArray(new List<string> { "EN", "DE" }),
                    Album = album,
                };
                var track3 = new Track
                {
                    Title = "Probably",
                    // ARRAY columns may also be null.
                    Lyrics = null,
                    LyricsLanguages = null,
                    Album = album,
                };
                await session.SaveAsync(track1);
                await session.SaveAsync(track2);
                await session.SaveAsync(track3);
                await transaction.CommitAsync();

                Console.WriteLine("Added 3 tracks.");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                throw;
            }
        }
        
        private static async Task<(Singer, Album)> GetSingerAndAlbumAsync(ISession session)
        {
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

            return (singer, album);
        }
    }
}