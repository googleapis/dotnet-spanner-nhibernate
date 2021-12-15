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
using System.Linq;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// The Clr type <see cref="DateTime"/> is often used for both dates and timestamps. Cloud Spanner has two distinct
    /// data types for DATE and TIMESTAMP. To distinguish between the two in NHibernate, it is recommended to
    /// use <see cref="SpannerDate"/> to map DATE columns and <see cref="DateTime"/> to map TIMESTAMP columns.
    /// 
    /// Run from the command line with `dotnet run Timestamp`
    /// </summary>
    public static class TimestampSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();

            // Create a concert.
            var (singer, venue) = await GetSingerAndVenueAsync(configuration);
            var concert = new Concert
            {
                Singer = singer,
                Venue = venue,
                // TIMESTAMP columns are mapped to DateTime by default. Cloud Spanner stores all TIMESTAMP values in UTC.
                // If a TIMESTAMP value is set in local time, the value will be converted to UTC before it is written to
                // Cloud Spanner.
                StartTime = new DateTime(2021, 2, 1, 19, 30, 0, DateTimeKind.Utc),
                Title = "Theodore in Concert Hall",
            };
            await session.SaveAsync(concert);
            await session.FlushAsync();

            // Commonly used methods and properties of DateTime are mapped to the equivalent Cloud Spanner functions
            // and can be used in queries.
            var concertsInFeb2021 = await session
                .Query<Concert>()
                .Where(c => c.StartTime.Month == 2 && c.StartTime.Year == 2021)
                .OrderBy(c => c.StartTime)
                .ToListAsync();
            foreach (var c in concertsInFeb2021)
            {
                Console.WriteLine($"February concert: {c.Title}, starts at {c.StartTime}");
            }
        }
        
        private static async Task<(Singer, Venue)> GetSingerAndVenueAsync(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            var venue = new Venue
            {
                Code = "CON",
                Name = "Concert Hall",
                Active = true,
            };
            await session.SaveAsync(venue);
            var singer = new Singer
            {
                FirstName = "Theodore",
                LastName = "Walterson",
            };
            await session.SaveAsync(singer);
            await transaction.CommitAsync();

            return (singer, venue);
        }        
    }
}