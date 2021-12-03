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

using Google.Cloud.Spanner.NHibernate.IntegrationTests.SampleEntities;
using Google.Cloud.Spanner.V1;
using NHibernate.Criterion;
using NHibernate.Linq;
using NHibernate.SqlCommand;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests
{
    [Collection(nameof(NonParallelTestCollection))]
    public class QueryTests : IClassFixture<SpannerSampleFixture>
    {
        private readonly SpannerSampleFixture _fixture;

        public QueryTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [Fact]
        public async Task CanFilterOnProperty()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = await session.SaveAsync(new Singer
            {
                FirstName = "Pete",
                LastName = "Peterson",
            });
            await session.FlushAsync();
            
            var singer = await session.Query<Singer>()
                .Where(s => s.FullName == "Pete Peterson" && s.Id == (string)id)
                .FirstOrDefaultAsync();
            Assert.NotNull(singer);
            Assert.Equal("Pete Peterson", singer.FullName);
        }

        [Fact]
        public async Task CanOrderByProperty()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = (string) await session.SaveAsync(new Singer { FirstName = "Pete", LastName = "Peterson" });
            var singerId2 = (string) await session.SaveAsync(new Singer { FirstName = "Zeke", LastName = "Allison" });
            await session.FlushAsync();

            var singers = await session.Query<Singer>()
                .Where(s => s.Id == singerId1 || s.Id == singerId2)
                .OrderBy(s => s.LastName)
                .ToListAsync();
            Assert.Collection(singers,
                s => Assert.Equal("Zeke Allison", s.FullName),
                s => Assert.Equal("Pete Peterson", s.FullName)
            );
        }

        [Fact]
        public async Task CanFetchAssociation()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singer1 = new Singer { FirstName = "Pete", LastName = "Peterson" };
            var singer2 = new Singer { FirstName = "Zeke", LastName = "Allison" };
            var singerId1 = (string) await session.SaveAsync(singer1);
            var singerId2 = (string) await session.SaveAsync(singer2);
            await session.SaveAsync(new Album { Title = "Album 1", Singer = singer1 });
            await session.SaveAsync(new Album { Title = "Album 2", Singer = singer1 });
            await session.SaveAsync(new Album { Title = "Album 3", Singer = singer2 });
            await session.SaveAsync(new Album { Title = "Album 4", Singer = singer2 });
            await session.SaveAsync(new Album { Title = "Album 5", Singer = singer2 });
            await session.FlushAsync();

            var albums = await session.Query<Album>()
                .Fetch(a => a.Singer)
                .Where(a => a.Singer.LastName == "Allison" && new [] { singerId1, singerId2 }.Contains(a.Singer.Id))
                .OrderBy(a => a.Title)
                .ToListAsync();
            Assert.Collection(albums,
                a => Assert.Equal("Album 3", a.Title),
                a => Assert.Equal("Album 4", a.Title),
                a => Assert.Equal("Album 5", a.Title)
            );
        }

        [Fact]
        public async Task CanUseLimitWithoutOffset()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = (string) await session.SaveAsync(new Singer { FirstName = "Pete", LastName = "Peterson" });
            var singerId2 = (string) await session.SaveAsync(new Singer { FirstName = "Zeke", LastName = "Allison" });
            await session.FlushAsync();

            var singers = await session.Query<Singer>()
                .Where(s => new [] { singerId1, singerId2 }.Contains(s.Id))
                .OrderBy(s => s.LastName)
                .Take(1)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Allison", s.LastName));
        }

        [Fact]
        public async Task CanUseLimitWithOffset()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = (string) await session.SaveAsync(new Singer { FirstName = "Pete", LastName = "Peterson" });
            var singerId2 = (string) await session.SaveAsync(new Singer { FirstName = "Zeke", LastName = "Allison" });
            var singerId3 = (string) await session.SaveAsync(new Singer { FirstName = "Sandra", LastName = "Ericson" });
            await session.FlushAsync();

            var singers = await session.Query<Singer>()
                .Where(s => new [] { singerId1, singerId2, singerId3 }.Contains(s.Id))
                .OrderBy(s => s.LastName)
                .Skip(1)
                .Take(1)
                .ToListAsync();

            Assert.Collection(singers,
                s => Assert.Equal("Ericson", s.LastName)
            );
        }

        [Fact]
        public async Task CanUseOffsetWithoutLimit()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = (string) await session.SaveAsync(new Singer { FirstName = "Pete", LastName = "Peterson" });
            var singerId2 = (string) await session.SaveAsync(new Singer { FirstName = "Zeke", LastName = "Allison" });
            await session.FlushAsync();

            var singers = await session.Query<Singer>()
                .Where(s => new [] { singerId1, singerId2 }.Contains(s.Id))
                .OrderBy(s => s.LastName)
                .Skip(1)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Peterson", s.LastName));
        }

        [Fact]
        public async Task CanUseInnerJoin()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singer1 = new Singer { FirstName = "Pete", LastName = "Peterson" };
            var singer2 = new Singer { FirstName = "Zeke", LastName = "Allison" };
            var singerId1 = (string) await session.SaveAsync(singer1);
            var singerId2 = (string) await session.SaveAsync(singer2);
            await session.SaveAsync(new Album { Title = "Some Title", Singer = singer1 });
            await session.FlushAsync();

            var singers = await session.Query<Singer>()
                .Join(session.Query<Album>(), s => s.Id, a => a.Singer.Id, (s, a) => new { Singer = s, Album = a })
                .Where(s => new[] { singerId1, singerId2 }.Contains(s.Singer.Id))
                .ToListAsync();

            Assert.Collection(singers,
                s =>
                {
                    Assert.Equal("Peterson", s.Singer.LastName);
                    Assert.Equal("Some Title", s.Album.Title);
                }
            );
        }

        [Fact]
        public async Task CanUseOuterJoin()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singer1 = new Singer { FirstName = "Pete", LastName = "Peterson" };
            var singer2 = new Singer { FirstName = "Zeke", LastName = "Allison" };
            var singerId1 = (string) await session.SaveAsync(singer1);
            var singerId2 = (string) await session.SaveAsync(singer2);
            await session.SaveAsync(new Album { Title = "Some Title", Singer = singer1 });
            await session.FlushAsync();

            var singers = await session.Query<Singer>()
                .GroupJoin(session.Query<Album>(), s => s.Id, a => a.Singer.Id, (s, a) => new { Singer = s, Albums = a })
                .SelectMany(
                    s => s.Albums.DefaultIfEmpty(),
                    (s, a) => new { s.Singer, Album = a })
                .Where(s => new [] { singerId1, singerId2 }.Contains(s.Singer.Id))
                .OrderBy(s => s.Singer.LastName)
                .ToListAsync();

            Assert.Collection(singers,
                s =>
                {
                    Assert.Equal("Allison", s.Singer.LastName);
                    Assert.Null(s.Album);
                },
                s =>
                {
                    Assert.Equal("Peterson", s.Singer.LastName);
                    Assert.Equal("Some Title", s.Album.Title);
                }
            );
        }

        [Fact]
        public async Task CanUseStringContains()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = (string) await session.SaveAsync(new Singer { FirstName = "Pete", LastName = "Peterson" });
            var singerId2 = (string) await session.SaveAsync(new Singer { FirstName = "Zeke", LastName = "Allison" });
            await session.FlushAsync();

            var fullName = "Alli";
            var singers = await session.Query<Singer>()
                .Where(s => s.FullName.Contains(fullName))
                .Where(s => new [] { singerId1, singerId2 }.Contains(s.Id))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Zeke Allison", s.FullName));
        }

        [Fact]
        public async Task CanUseStringStartsWith()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = (string) await session.SaveAsync(new Singer { FirstName = "Pete", LastName = "Peterson" });
            var singerId2 = (string) await session.SaveAsync(new Singer { FirstName = "Zeke", LastName = "Allison" });
            await session.FlushAsync();

            var fullName = "Zeke";
            var singers = await session.Query<Singer>()
                .Where(s => s.FullName.StartsWith(fullName))
                .Where(s => new [] { singerId1, singerId2 }.Contains(s.Id))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Zeke Allison", s.FullName));
        }

        [Fact]
        public async Task CanUseStringEndsWith()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = (string) await session.SaveAsync(new Singer { FirstName = "Pete", LastName = "Peterson" });
            var singerId2 = (string) await session.SaveAsync(new Singer { FirstName = "Zeke", LastName = "Allison" });
            await session.FlushAsync();

            var fullName = "Peterson";
            var singers = await session.Query<Singer>()
                .Where(s => s.FullName.EndsWith(fullName))
                .Where(s => new [] { singerId1, singerId2 }.Contains(s.Id))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Pete Peterson", s.FullName));
        }

        [Fact]
        public async Task CanUseStringLength()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            var singerId2 = (string) await session.SaveAsync(new Singer { FirstName = "Zeke", LastName = "Allison" });
            await session.FlushAsync();

            var minLength = 4;
            var singers = await session.Query<Singer>()
                .Where(s => s.FirstName.Length > minLength)
                .Where(s => new [] { singerId1, singerId2 }.Contains(s.Id))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Alice Morrison", s.FullName));
        }

        [Fact]
        public async Task CanUseStringIndexOf()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = (string) await session.SaveAsync(new Singer { FirstName = "Pete", LastName = "Peterson" });
            var singerId2 = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var name = "Morrison";
            var minIndex = 0;
            var singers = await session.Query<Singer>()
                .Where(s => s.FullName.IndexOf(name) > minIndex)
                .Where(s => new [] { singerId1, singerId2 }.Contains(s.Id))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Alice Morrison", s.FullName));
        }

        [Fact]
        public async Task CanUseStringReplace()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = (string) await session.SaveAsync(new Singer { FirstName = "Pete", LastName = "Peterson" });
            var singerId2 = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var from = "Pete";
            var to = "Peter";
            var name = "Peter Peterrson";
            var singers = await session.Query<Singer>()
                .Where(s => s.FullName.Replace(from, to) == name)
                .Where(s => new [] { singerId1, singerId2 }.Contains(s.Id))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Pete Peterson", s.FullName));
        }

        [Fact]
        public async Task CanUseStringToLower()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var fullNameLowerCase = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.ToLower())
                .FirstOrDefaultAsync();

            Assert.Equal("alice morrison", fullNameLowerCase);
        }

        [Fact]
        public async Task CanUseStringToUpper()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var fullNameLowerCase = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.ToUpper())
                .FirstOrDefaultAsync();

            Assert.Equal("ALICE MORRISON", fullNameLowerCase);
        }

        [Fact]
        public async Task CanUseStringSubstring()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var lastNameFromFullName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.Substring(6))
                .FirstOrDefaultAsync();

            Assert.Equal("Morrison", lastNameFromFullName);
        }

        [Fact]
        public async Task CanUseStringSubstringWithLength()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var lastNameFromFullName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.Substring(6, 3))
                .FirstOrDefaultAsync();

            Assert.Equal("Mor", lastNameFromFullName);
        }

        [Fact]
        public async Task CanUseStringTrimStart()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "   Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var trimmedName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.TrimStart())
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringTrimStartWithArgument()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "\t\t\tAlice", LastName = "Morrison" });
            await session.FlushAsync();

            var trimmedName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.TrimStart('\t'))
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringTrimEnd()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison   " });
            await session.FlushAsync();

            var trimmedName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.TrimEnd())
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringTrimEndWithArgument()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison\t\t\t" });
            await session.FlushAsync();

            var trimmedName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.TrimEnd('\t'))
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringTrim()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison   " });
            await session.FlushAsync();

            var trimmedName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.Trim())
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringTrimWithArgument()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "\t\t\tAlice", LastName = "Morrison\t\t\t" });
            await session.FlushAsync();

            var trimmedName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.Trim('\t'))
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringConcat()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var calculatedFullName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FirstName + " " + s.LastName)
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", calculatedFullName);
        }

        [Fact]
        public async Task CanUseStringPadLeft()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var paddedName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.PadLeft(20))
                .FirstOrDefaultAsync();

            Assert.Equal("      Alice Morrison", paddedName);
        }

        [Fact]
        public async Task CanUseStringPadLeftWithChar()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var paddedName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.PadLeft(20, '$'))
                .FirstOrDefaultAsync();

            Assert.Equal("$$$$$$Alice Morrison", paddedName);
        }

        [Fact]
        public async Task CanUseStringPadRight()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var paddedName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.PadRight(20))
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison      ", paddedName);
        }

        [Fact]
        public async Task CanUseStringPadRightWithChar()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId = (string) await session.SaveAsync(new Singer { FirstName = "Alice", LastName = "Morrison" });
            await session.FlushAsync();

            var paddedName = await session.Query<Singer>()
                .Where(s => new [] { singerId }.Contains(s.Id))
                .Select(s => s.FullName.PadRight(20, '$'))
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison$$$$$$", paddedName);
        }

        [Fact]
        public async Task CanUseDateTimeAddDays()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = _fixture.RandomLong();
            var timestamp = new DateTime(2021, 1, 21, 11, 40, 10, DateTimeKind.Utc);
            await session.SaveAsync(new TableWithAllColumnTypes { ColInt64 = id, ColTimestamp = timestamp });
            await session.FlushAsync();

            var date = await session.Query<TableWithAllColumnTypes>()
                .Where(s => s.ColInt64 == id)
                .Select(s => new { D1 = ((DateTime)s.ColTimestamp).AddDays(23), D2 = ((DateTime)s.ColTimestamp).AddDays(100) })
                .FirstOrDefaultAsync();

            Assert.Equal(timestamp.AddDays(23), date.D1);
            Assert.Equal(timestamp.AddDays(100), date.D2);
        }

        [Fact]
        public async Task CanUseDateTimeAddHours()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = _fixture.RandomLong();
            var timestamp = new DateTime(2021, 1, 21, 11, 40, 10, DateTimeKind.Utc);
            await session.SaveAsync(new TableWithAllColumnTypes { ColInt64 = id, ColTimestamp = timestamp });
            await session.FlushAsync();

            var date = await session.Query<TableWithAllColumnTypes>()
                .Where(s => s.ColInt64 == id)
                .Select(s => ((DateTime)s.ColTimestamp).AddHours(47))
                .FirstOrDefaultAsync();

            Assert.Equal(new DateTime(2021, 1, 23, 10, 40, 10, DateTimeKind.Utc), date);
        }

        [Fact]
        public async Task CanUseDateTimeAddTicks()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = _fixture.RandomLong();
            var timestamp = new DateTime(2021, 1, 21, 11, 40, 10, DateTimeKind.Utc);
            await session.SaveAsync(new TableWithAllColumnTypes { ColInt64 = id, ColTimestamp = timestamp });
            await session.FlushAsync();

            var date = await session.Query<TableWithAllColumnTypes>()
                .Where(s => s.ColInt64 == id)
                .Select(s => ((DateTime)s.ColTimestamp).AddTicks(20))
                .FirstOrDefaultAsync();

            Assert.Equal(new DateTime(2021, 1, 21, 11, 40, 10, DateTimeKind.Utc).AddTicks(20), date);
        }

        [Fact]
        public async Task CanUseDateTimeProperties()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = _fixture.RandomLong();
            var timestamp = new DateTime(2021, 1, 25, 14, 29, 15, 182, DateTimeKind.Utc);
            await session.SaveAsync(new TableWithAllColumnTypes { ColInt64 = id, ColTimestamp = timestamp });
            await session.FlushAsync();

            var extracted = await session.Query<TableWithAllColumnTypes>()
                .Where(t => t.ColInt64 == id)
                .Select(t => new
                {
                    t.ColTimestamp.Value.Year,
                    t.ColTimestamp.Value.Month,
                    t.ColTimestamp.Value.Day,
                    t.ColTimestamp.Value.DayOfYear,
                    t.ColTimestamp.Value.DayOfWeek,
                    t.ColTimestamp.Value.Hour,
                    t.ColTimestamp.Value.Minute,
                    t.ColTimestamp.Value.Second,
                    t.ColTimestamp.Value.Millisecond,
                })
                .FirstOrDefaultAsync();
            Assert.Equal(timestamp.Year, extracted.Year);
            Assert.Equal(timestamp.Month, extracted.Month);
            Assert.Equal(timestamp.Day, extracted.Day);
            Assert.Equal(timestamp.DayOfYear, extracted.DayOfYear);
            Assert.Equal(timestamp.DayOfWeek, extracted.DayOfWeek);
            Assert.Equal(timestamp.Hour, extracted.Hour);
            Assert.Equal(timestamp.Minute, extracted.Minute);
            Assert.Equal(timestamp.Second, extracted.Second);
            Assert.Equal(timestamp.Millisecond, extracted.Millisecond);
        }

        [Fact]
        public async Task CanUseBoolToString()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = _fixture.RandomLong();
            await session.SaveAsync(new TableWithAllColumnTypes { ColInt64 = id, ColBool = true });
            await session.FlushAsync();

            var converted = await session.Query<TableWithAllColumnTypes>()
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColBool.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("true", converted);
        }

        [Fact]
        public async Task CanUseBytesToString()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = _fixture.RandomLong();
            await session.SaveAsync(new TableWithAllColumnTypes { ColInt64 = id, ColBytes = Encoding.UTF8.GetBytes("test") });
            await session.FlushAsync();

            var converted = await session.Query<TableWithAllColumnTypes>()
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColBytes.ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("test", converted);
        }

        [Fact]
        public async Task CanUseLongToString()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = _fixture.RandomLong();
            await session.SaveAsync(new TableWithAllColumnTypes { ColInt64 = id });
            await session.FlushAsync();

            var converted = await session.Query<TableWithAllColumnTypes>()
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColInt64.ToString())
                .FirstOrDefaultAsync();
            Assert.Equal($"{id}", converted);
        }

        [Fact]
        public async Task CanUseSpannerNumericToString()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = _fixture.RandomLong();
            await session.SaveAsync(new TableWithAllColumnTypes { ColInt64 = id, ColNumeric = new SpannerNumeric(V1.SpannerNumeric.Parse("3.14")) });
            await session.FlushAsync();

            var converted = await session.Query<TableWithAllColumnTypes>()
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColNumeric.Value.ToString())
                .FirstOrDefaultAsync();
            // The emulator and real Spanner have a slight difference in casting this FLOAT64 to STRING.
            // Real Spanner returns '3.14' and the emulator returns '3.1400000000000001'.
            Assert.StartsWith("3.14", converted);
        }

        [Fact]
        public async Task CanUseDoubleToString()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = _fixture.RandomLong();
            await session.SaveAsync(new TableWithAllColumnTypes { ColInt64 = id, ColFloat64 = 3.14d });
            await session.FlushAsync();

            var converted = await session.Query<TableWithAllColumnTypes>()
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColFloat64.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            // The emulator and real Spanner have a slight difference in casting this FLOAT64 to STRING.
            // Real Spanner returns '3.14' and the emulator returns '3.1400000000000001'.
            Assert.StartsWith("3.14", converted);
        }

        [Fact]
        public async Task CanUseSpannerDateToString()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = _fixture.RandomLong();
            await session.SaveAsync(new TableWithAllColumnTypes { ColInt64 = id, ColDate = new SpannerDate(2021, 1, 25) });
            await session.FlushAsync();

            var converted = await session.Query<TableWithAllColumnTypes>()
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColDate.ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("2021-01-25", converted);
        }

        [Fact]
        public async Task CanUseDateTimeToString()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = _fixture.RandomLong();
            await session.SaveAsync(new TableWithAllColumnTypes { ColInt64 = id, ColTimestamp = new DateTime(2021, 1, 25, 12, 46, 1, 982, DateTimeKind.Utc) });
            await session.FlushAsync();

            var converted = await session.Query<TableWithAllColumnTypes>()
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColTimestamp.ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("2021-01-25 04:46:01.982-08", converted);
        }

        [Fact]
        public async Task CanQueryRawSqlWithParameters()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = (string) await session.SaveAsync(new Singer { FirstName = "Pete", LastName = "Peterson" });
            var singerId2 = (string) await session.SaveAsync(new Singer { FirstName = "Zeke", LastName = "Allison" });
            await session.FlushAsync();

            var singers = await session
                .CreateSQLQuery("SELECT * FROM Singers WHERE Id IN UNNEST(:id) ORDER BY LastName")
                .AddEntity(typeof(Singer))
                .SetParameter("id", new SpannerStringArray(new List<string> { singerId1, singerId2 }))
                .ListAsync<Singer>();
            Assert.Collection(singers,
                s => Assert.Equal("Allison", s.LastName),
                s => Assert.Equal("Peterson", s.LastName)
            );
        }

        [Fact]
        public async Task CanInsertSingerUsingRawSql()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singerId1 = Guid.NewGuid().ToString();
            var firstName1 = "Pete";
            var lastName1 = "Peterson";
            var updateCount1 = await session
                .CreateSQLQuery(
                    "INSERT INTO Singers (Id, FirstName, LastName) VALUES (:id, :firstName, :lastName)")
                .SetParameter("id", singerId1)
                .SetParameter("firstName", firstName1)
                .SetParameter("lastName", lastName1)
                .ExecuteUpdateAsync();

            var singerId2 = Guid.NewGuid().ToString();
            var firstName2 = "Zeke";
            var lastName2 = "Allison";
            var updateCount2 = await session
                .CreateSQLQuery(
                    $"INSERT INTO Singers (Id, FirstName, LastName) VALUES ('{singerId2}', '{firstName2}', '{lastName2}')")
                .ExecuteUpdateAsync();

            Assert.Equal(1, updateCount1);
            Assert.Equal(1, updateCount2);

            var singers = await session
                .CreateSQLQuery("SELECT * FROM Singers WHERE Id IN UNNEST(:id) ORDER BY LastName")
                .AddEntity(typeof(Singer))
                .SetParameter("id", new SpannerStringArray(new List<string>{singerId1, singerId2}))
                .ListAsync<Singer>();
            Assert.Collection(singers,
                s => Assert.Equal("Allison", s.LastName),
                s => Assert.Equal("Peterson", s.LastName)
            );
        }

        [Fact]
        public async Task CanInsertRowWithAllColumnTypesUsingRawSql()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id1 = _fixture.RandomLong();
            var today = SpannerDate.FromDateTime(DateTime.SpecifyKind(DateTime.Today, DateTimeKind.Unspecified));
            var now = DateTime.UtcNow;

            var row = new TableWithAllColumnTypes
            {
                ColBool = true,
                ColBoolArray = new SpannerBoolArray(new List<bool?> { true, false, true }),
                ColBytes = new byte[] { 1, 2, 3 },
                ColBytesMax = Encoding.UTF8.GetBytes("This is a long string"),
                ColBytesArray = new SpannerBytesArray(new List<byte[]> { new byte[] { 3, 2, 1 }, new byte[] { }, new byte[] { 4, 5, 6 } }),
                ColBytesMaxArray = new SpannerBytesArray(new List<byte[]> { Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2"), Encoding.UTF8.GetBytes("string 3") }),
                ColDate = new SpannerDate(2020, 12, 28),
                ColDateArray = new SpannerDateArray(new List<DateTime?> { new SpannerDate(2020, 12, 28).ToDateTime(), new SpannerDate(2010, 1, 1).ToDateTime(), today.ToDateTime() }),
                ColFloat64 = 3.14D,
                ColFloat64Array = new SpannerFloat64Array(new List<double?> { 3.14D, 6.626D }),
                ColInt64 = id1,
                ColInt64Array = new SpannerInt64Array(new List<long?> { 1L, 2L, 4L, 8L }),
                ColJson = new SpannerJson("{\"key\": \"value\"}"),
                ColJsonArray = new SpannerJsonArray(new List<string>{"{\"key1\": \"value1\"}", null, "{\"key2\": \"value2\"}"}),
                ColNumeric = new SpannerNumeric((V1.SpannerNumeric)3.14m),
                ColNumericArray = new SpannerNumericArray(new List<V1.SpannerNumeric?> { (V1.SpannerNumeric)3.14m, (V1.SpannerNumeric)6.626m }),
                ColString = "some string",
                ColStringArray = new SpannerStringArray(new List<string> { "string1", "string2", "string3" }),
                ColStringMax = "some longer string",
                ColStringMaxArray = new SpannerStringArray(new List<string> { "longer string1", "longer string2", "longer string3" }),
                ColTimestamp = new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288),
                ColTimestampArray = new SpannerTimestampArray(new List<DateTime?> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now }),
            };
            var updateCount1 = await session
                .CreateSQLQuery(@"INSERT INTO TableWithAllColumnTypes 
                              (ColBool, ColBoolArray, ColBytes, ColBytesMax, ColBytesArray, ColBytesMaxArray,
                               ColDate, ColDateArray, ColFloat64, ColFloat64Array, ColInt64, ColInt64Array,
                               /*ColJson, ColJsonArray,*/ ColNumeric, ColNumericArray, ColString, ColStringArray,
                               ColStringMax, ColStringMaxArray, ColTimestamp, ColTimestampArray)
                              VALUES
                              (:ColBool, :ColBoolArray, :ColBytes, :ColBytesMax, :ColBytesArray, :ColBytesMaxArray,
                               :ColDate, :ColDateArray, :ColFloat64, :ColFloat64Array, :ColInt64, :ColInt64Array,
                               /*:ColJson, :ColJsonArray,*/ :ColNumeric, :ColNumericArray, :ColString, :ColStringArray,
                               :ColStringMax, :ColStringMaxArray, :ColTimestamp, :ColTimestampArray)")
                .SetParameter("ColBool", row.ColBool)
                .SetParameter("ColBoolArray", row.ColBoolArray)
                .SetParameter("ColBytes", row.ColBytes)
                .SetParameter("ColBytesMax", row.ColBytesMax)
                .SetParameter("ColBytesArray", row.ColBytesArray)
                .SetParameter("ColBytesMaxArray", row.ColBytesMaxArray)
                .SetParameter("ColDate", row.ColDate)
                .SetParameter("ColDateArray", row.ColDateArray)
                .SetParameter("ColFloat64", row.ColFloat64)
                .SetParameter("ColFloat64Array", row.ColFloat64Array)
                .SetParameter("ColInt64", row.ColInt64)
                .SetParameter("ColInt64Array", row.ColInt64Array)
                // .SetParameter("ColJson", row.ColJson)
                // .SetParameter("ColJsonArray", row.ColJsonArray)
                .SetParameter("ColNumeric", row.ColNumeric)
                .SetParameter("ColNumericArray", row.ColNumericArray)
                .SetParameter("ColString", row.ColString)
                .SetParameter("ColStringArray", row.ColStringArray)
                .SetParameter("ColStringMax", row.ColStringMax)
                .SetParameter("ColStringMaxArray", row.ColStringMaxArray)
                .SetParameter("ColTimestamp", row.ColTimestamp)
                .SetParameter("ColTimestampArray", row.ColTimestampArray)
                .ExecuteUpdateAsync();
            Assert.Equal(1, updateCount1);

            var rows = await session
                .CreateSQLQuery("SELECT * FROM TableWithAllColumnTypes WHERE ColInt64 IN UNNEST(:id) ORDER BY ColString")
                .AddEntity(typeof(TableWithAllColumnTypes))
                .SetParameter("id", new SpannerInt64Array(new List<long?>{id1}))
                .ListAsync<TableWithAllColumnTypes>();
            Assert.Collection(rows,
                row => Assert.NotNull(row.ColDateArray)
            );
        }

        [Fact]
        public async Task CanUseStatementHint()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = (string) await session.SaveAsync(new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            });
            await session.FlushAsync();
            var singers = await session
                .Query<Singer>()
                .SetStatementHint("@{USE_ADDITIONAL_PARALLELISM=TRUE}")
                .Where(s => s.LastName.Equals("Peterson") && s.Id.Equals(id))
                .ToListAsync();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", singer.FullName));
        }

        [Fact]
        public async Task CanUseTableHint()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = (string) await session.SaveAsync(new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            });
            await session.FlushAsync();
            var singers = await session
                .Query<Singer>()
                .SetTableHint("Singers", "@{FORCE_INDEX=Idx_Singers_FullName}")
                .Where(s => s.LastName.Equals("Peterson") && s.Id.Equals(id))
                .ToListAsync();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", singer.FullName));
        }

        [Fact]
        public async Task CanUseTableHintOnJoin()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singer = new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            };
            var id = (string) await session.SaveAsync(singer);
            await session.SaveAsync(new Album
            {
                Singer = singer, Title = "My first album"
            });
            await session.FlushAsync();
            var singers = await session
                .Query<Album>()
                .Select(a => a.Singer)
                .SetTableHints(new Dictionary<string, string>
                {
                    {"Singers", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Albums", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .Where(s => s.LastName.Equals("Peterson") && s.Id.Equals(id))
                .ToListAsync();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", singer.FullName));
        }

        [Fact]
        public async Task CanUseStatementAndTableHints()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var singer = new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            };
            var id = (string) await session.SaveAsync(singer);
            await session.SaveAsync(new Album
            {
                Singer = singer, Title = "My first album"
            });
            await session.FlushAsync();
            var singers = await session
                .Query<Album>()
                .Select(a => a.Singer)
                .SetStatementAndTableHints("@{USE_ADDITIONAL_PARALLELISM=TRUE}", new Dictionary<string, string>
                {
                    {"Singer", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Album", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .Where(s => s.LastName.Equals("Peterson") && s.Id.Equals(id))
                .ToListAsync();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", singer.FullName));
        }

        [Fact]
        public async Task CanUseStatementHintWithHql()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = (string) await session.SaveAsync(new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            });
            await session.FlushAsync();
            var singers = await session
                .CreateQuery("from Singer where LastName = :lastName and Id = :id")
                .SetStatementHint("@{USE_ADDITIONAL_PARALLELISM=TRUE}")
                .SetParameter("lastName", "Peterson")
                .SetParameter("id", id)
                .ListAsync<Singer>();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", singer.FullName));
        }

        [Fact]
        public async Task CanUseTableHintWithHql()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = (string) await session.SaveAsync(new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            });
            await session.FlushAsync();
            var singers = await session
                .CreateQuery("from Singer where LastName = :lastName and Id = :id")
                .SetTableHint("Singers", "@{FORCE_INDEX=Idx_Singers_FullName}")
                .SetParameter("lastName", "Peterson")
                .SetParameter("id", id)
                .ListAsync<Singer>();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", singer.FullName));
        }

        [Fact]
        public async Task CanUseTableHintOnJoinWithHql()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = (string) await session.SaveAsync(new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            });
            await session.FlushAsync();
            var singers = await session
                .CreateQuery("from Singer as singer left outer join singer.Albums as album where singer.LastName = :lastName and singer.Id = :id")
                .SetTableHints(new Dictionary<string, string>
                {
                    {"Singers", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Albums", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .SetParameter("lastName", "Peterson")
                .SetParameter("id", id)
                .ListAsync<object[]>();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", (singer[0] as Singer)!.FullName));
        }

        [Fact]
        public async Task CanUseStatementAndTableHintsWithHql()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = (string) await session.SaveAsync(new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            });
            await session.FlushAsync();
            var singers = await session
                .CreateQuery("from Singer as singer left outer join singer.Albums as album where singer.LastName = :lastName and singer.Id = :id")
                .SetStatementAndTableHints("@{USE_ADDITIONAL_PARALLELISM=TRUE}", new Dictionary<string, string>
                {
                    {"Singers", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Albums", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .SetParameter("lastName", "Peterson")
                .SetParameter("id", id)
                .ListAsync<object[]>();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", (singer[0] as Singer)!.FullName));
        }

        [Fact]
        public async Task CanUseStatementHintWithCriteria()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = await session.SaveAsync(new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            });
            await session.FlushAsync();
            var singers = await session
                .CreateCriteria(typeof(Singer))
                .Add(Restrictions.Eq("LastName", "Peterson"))
                .Add(Restrictions.Eq("Id", id))
                .SetStatementHint("@{USE_ADDITIONAL_PARALLELISM=TRUE}")
                .ListAsync<Singer>();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", singer.FullName));
        }

        [Fact]
        public async Task CanUseTableHintWithCriteria()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = await session.SaveAsync(new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            });
            await session.FlushAsync();
            var singers = await session
                .CreateCriteria(typeof(Singer))
                .Add(Restrictions.Eq("LastName", "Peterson"))
                .Add(Restrictions.Eq("Id", id))
                .SetTableHint("Singers", "@{FORCE_INDEX=Idx_Singers_FullName}")
                .ListAsync<Singer>();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", singer.FullName));
        }

        [Fact]
        public async Task CanUseTableHintOnJoinWithCriteria()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = await session.SaveAsync(new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            });
            await session.FlushAsync();
            var singers = await session
                .CreateCriteria(typeof(Singer), "singer")
                .CreateAlias("Albums", "album", JoinType.LeftOuterJoin)
                .Add(Restrictions.Eq("singer.LastName", "Peterson"))
                .Add(Restrictions.Eq("singer.Id", id))
                .SetTableHints(new Dictionary<string, string>
                {
                    {"Singers", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Albums", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .ListAsync<Singer>();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", singer.FullName));
        }

        [Fact]
        public async Task CanUseStatementAndTableHintsWithCriteria()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            var id = await session.SaveAsync(new Singer
            {
                FirstName = "Pete", LastName = "Peterson"
            });
            await session.FlushAsync();
            var singers = await session
                .CreateCriteria(typeof(Singer), "singer")
                .CreateAlias("Albums", "album", JoinType.LeftOuterJoin)
                .Add(Restrictions.Eq("singer.LastName", "Peterson"))
                .Add(Restrictions.Eq("singer.Id", id))
                .SetStatementAndTableHints("@{USE_ADDITIONAL_PARALLELISM=TRUE}", new Dictionary<string, string>
                {
                    {"Singers", "@{FORCE_INDEX=Idx_Singers_FullName}"},
                    {"Albums", "@{FORCE_INDEX=Idx_Albums_Title}"}
                })
                .ListAsync<Singer>();
            Assert.Collection(singers, singer => Assert.Equal("Pete Peterson", singer.FullName));
        }
    }
}
