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
using Google.Cloud.Spanner.NHibernate.IntegrationTests.SampleEntities;
using Google.Cloud.Spanner.V1;
using NHibernate.Id;
using NHibernate.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests
{
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
/*
        [Fact]
        public async Task CanUseDateTimeAddDays()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            var timestamp = new DateTime(2021, 1, 21, 11, 40, 10, DateTimeKind.Utc);
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColTimestamp = timestamp }
            );
            await db.SaveChangesAsync();

            var date = await db.TableWithAllColumnTypes
                .Where(s => s.ColInt64 == id)
                .Select(s => new { D1 = ((DateTime)s.ColTimestamp).AddDays(23), D2 = ((DateTime)s.ColTimestamp).AddDays(100) })
                .FirstOrDefaultAsync();

            Assert.Equal(timestamp.AddDays(23), date.D1);
            Assert.Equal(timestamp.AddDays(100), date.D2);
        }

        [Fact]
        public async Task CanUseDateTimeAddHours()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColTimestamp = new DateTime(2021, 1, 21, 11, 40, 10, DateTimeKind.Utc) }
            );
            await db.SaveChangesAsync();

            var date = await db.TableWithAllColumnTypes
                .Where(s => s.ColInt64 == id)
                .Select(s => ((DateTime)s.ColTimestamp).AddHours(47))
                .FirstOrDefaultAsync();

            Assert.Equal(new DateTime(2021, 1, 23, 10, 40, 10, DateTimeKind.Utc), date);
        }

        [Fact]
        public async Task CanUseDateTimeAddTicks()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColTimestamp = new DateTime(2021, 1, 21, 11, 40, 10, DateTimeKind.Utc) }
            );
            await db.SaveChangesAsync();

            var date = await db.TableWithAllColumnTypes
                .Where(s => s.ColInt64 == id)
                .Select(s => ((DateTime)s.ColTimestamp).AddTicks(20))
                .FirstOrDefaultAsync();

            Assert.Equal(new DateTime(2021, 1, 21, 11, 40, 10, DateTimeKind.Utc).AddTicks(20), date);
        }

        [Fact]
        public async Task CanUseNumericValueOrDefaultAsDecimal_ThenRoundWithDigits()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColNumeric = V1.SpannerNumeric.FromDecimal(3.14m, LossOfPrecisionHandling.Throw) }
            );
            await db.SaveChangesAsync();

            var expectedValue = 3.1m;
            var dbValue = await db.TableWithAllColumnTypes
                // Only rounding with the option AwayFromZero can be handled server side, as that is the only option offered by
                // Cloud Spanner. If the user does not specify this rounding mode, this query would fail as it cannot be constructed.
                .Where(s => s.ColInt64 == id && Math.Round(s.ColNumeric.GetValueOrDefault().ToDecimal(LossOfPrecisionHandling.Throw), 1, MidpointRounding.AwayFromZero) == expectedValue)
                .Select(s => Math.Round(s.ColNumeric.GetValueOrDefault().ToDecimal(LossOfPrecisionHandling.Throw), 1, MidpointRounding.AwayFromZero))
                .FirstOrDefaultAsync();

            Assert.Equal(expectedValue, dbValue);
        }

        [Fact]
        public async Task CanUseLongMax()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            var randomLong = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id }
            );
            await db.SaveChangesAsync();

            var expectedValue = Math.Max(id, randomLong);
            var dbValue = await db.TableWithAllColumnTypes
                .Where(s => s.ColInt64 == id && Math.Max(s.ColInt64, randomLong) == expectedValue)
                .Select(s => Math.Max(s.ColInt64, randomLong))
                .FirstOrDefaultAsync();

            Assert.Equal(expectedValue, dbValue);
        }

        [Fact]
        public async Task CanUseDateTimeProperties()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            var randomLong = _fixture.RandomLong();
            var timestamp = new DateTime(2021, 1, 25, 14, 29, 15, 182, DateTimeKind.Utc);
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColTimestamp = timestamp }
            );
            await db.SaveChangesAsync();

            var extracted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => new
                {
                    t.ColTimestamp.GetValueOrDefault().Year,
                    t.ColTimestamp.GetValueOrDefault().Month,
                    t.ColTimestamp.GetValueOrDefault().Day,
                    t.ColTimestamp.GetValueOrDefault().DayOfYear,
                    t.ColTimestamp.GetValueOrDefault().DayOfWeek,
                    t.ColTimestamp.GetValueOrDefault().Hour,
                    t.ColTimestamp.GetValueOrDefault().Minute,
                    t.ColTimestamp.GetValueOrDefault().Second,
                    t.ColTimestamp.GetValueOrDefault().Millisecond,
                    t.ColTimestamp.GetValueOrDefault().Date,
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
            Assert.Equal(timestamp.Date, extracted.Date);
        }

        [Fact]
        public async Task CanUseSpannerDateProperties()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            var randomLong = _fixture.RandomLong();
            var date = new SpannerDate(2021, 1, 25);
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColDate = date }
            );
            await db.SaveChangesAsync();

            var extracted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => new
                {
                    t.ColDate.GetValueOrDefault().Year,
                    t.ColDate.GetValueOrDefault().Month,
                    t.ColDate.GetValueOrDefault().Day,
                    t.ColDate.GetValueOrDefault().DayOfYear,
                    t.ColDate.GetValueOrDefault().DayOfWeek,
                })
                .FirstOrDefaultAsync();
            Assert.Equal(date.Year, extracted.Year);
            Assert.Equal(date.Month, extracted.Month);
            Assert.Equal(date.Day, extracted.Day);
            Assert.Equal(date.DayOfYear, extracted.DayOfYear);
            Assert.Equal(date.DayOfWeek, extracted.DayOfWeek);
        }

        [Fact]
        public async Task CanUseBoolToString()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColBool = true }
            );
            await db.SaveChangesAsync();

            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColBool.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("true", converted);
        }

        [Fact]
        public async Task CanUseBytesToString()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColBytes = Encoding.UTF8.GetBytes("test") }
            );
            await db.SaveChangesAsync();

            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColBytes.ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("test", converted);
        }

        [Fact]
        public async Task CanUseLongToString()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id }
            );
            await db.SaveChangesAsync();

            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColInt64.ToString())
                .FirstOrDefaultAsync();
            Assert.Equal($"{id}", converted);
        }

        [Fact]
        public async Task CanUseSpannerNumericToString()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColNumeric = V1.SpannerNumeric.Parse("3.14") }
            );
            await db.SaveChangesAsync();

            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColNumeric.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            // The emulator and real Spanner have a slight difference in casting this FLOAT64 to STRING.
            // Real Spanner returns '3.14' and the emulator returns '3.1400000000000001'.
            Assert.StartsWith("3.14", converted);
        }

        [Fact]
        public async Task CanUseDoubleToString()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColFloat64 = 3.14d }
            );
            await db.SaveChangesAsync();

            var converted = await db.TableWithAllColumnTypes
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
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColDate = new SpannerDate(2021, 1, 25) }
            );
            await db.SaveChangesAsync();

            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColDate.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("2021-01-25", converted);
        }

        [Fact]
        public async Task CanUseDateTimeToString()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColTimestamp = new DateTime(2021, 1, 25, 12, 46, 1, 982, DateTimeKind.Utc) }
            );
            await db.SaveChangesAsync();

            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColTimestamp.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("2021-01-25T12:46:01.982Z", converted);
        }

        [Fact]
        public async Task CanFilterOnArrayLength()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = id, ColStringArray = new List<string> { "1", "2" } }
            );
            await db.SaveChangesAsync();

            var selectedId = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id && t.ColStringArray.Count == 2)
                .Select(t => t.ColInt64)
                .FirstOrDefaultAsync();
            Assert.Equal(id, selectedId);
        }

        [Fact]
        public async Task CanQueryRawSqlWithParameters()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" }
            );
            await db.SaveChangesAsync();

            var singers = await db.Singers
                .FromSqlRaw("SELECT * FROM Singers WHERE SingerId IN UNNEST(@id)", new SpannerParameter("id", SpannerDbType.ArrayOf(SpannerDbType.Int64), new List<long> { singerId1, singerId2 }))
                .OrderBy(s => s.LastName)
                .ToListAsync();
            Assert.Collection(singers,
                s => Assert.Equal("Allison", s.LastName),
                s => Assert.Equal("Peterson", s.LastName)
            );
        }

        [Fact]
        public async Task CanInsertSingerUsingRawSql()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var firstName1 = "Pete";
            var lastName1 = "Peterson";
            var updateCount1 = await db.Database.ExecuteSqlRawAsync(
                "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (@id, @firstName, @lastName)",
                new SpannerParameter("id", SpannerDbType.Int64, singerId1),
                new SpannerParameter("firstName", SpannerDbType.String, firstName1),
                new SpannerParameter("lastName", SpannerDbType.String, lastName1)
            );

            var singerId2 = _fixture.RandomLong();
            var firstName2 = "Zeke";
            var lastName2 = "Allison";
            var updateCount2 = await db.Database.ExecuteSqlInterpolatedAsync(
                $"INSERT INTO Singers (SingerId, FirstName, LastName) VALUES ({singerId2}, {firstName2}, {lastName2})"
            );

            var singerId3 = _fixture.RandomLong();
            var firstName3 = "Luke";
            var lastName3 = "Harrison";
            var updateCount3 = await db.Database.ExecuteSqlRawAsync(
                "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES ({0}, {1}, {2})",
                singerId3, firstName3, lastName3
            );

            Assert.Equal(1, updateCount1);
            Assert.Equal(1, updateCount2);
            Assert.Equal(1, updateCount3);

            var singers = await db.Singers
                .FromSqlRaw("SELECT * FROM Singers WHERE SingerId IN UNNEST(@id)", new SpannerParameter("id", SpannerDbType.ArrayOf(SpannerDbType.Int64), new List<long> { singerId1, singerId2, singerId3 }))
                .OrderBy(s => s.LastName)
                .ToListAsync();
            Assert.Collection(singers,
                s => Assert.Equal("Allison", s.LastName),
                s => Assert.Equal("Harrison", s.LastName),
                s => Assert.Equal("Peterson", s.LastName)
            );
        }

        [Fact]
        public async Task CanInsertRowWithAllColumnTypesUsingRawSql()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id1 = _fixture.RandomLong();
            var today = SpannerDate.FromDateTime(DateTime.SpecifyKind(DateTime.Today, DateTimeKind.Unspecified));
            var now = DateTime.UtcNow;

            var row = new TableWithAllColumnTypes
            {
                ColBool = true,
                ColBoolArray = new List<bool?> { true, false, true },
                ColBytes = new byte[] { 1, 2, 3 },
                ColBytesMax = Encoding.UTF8.GetBytes("This is a long string"),
                ColBytesArray = new List<byte[]> { new byte[] { 3, 2, 1 }, new byte[] { }, new byte[] { 4, 5, 6 } },
                ColBytesMaxArray = new List<byte[]> { Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2"), Encoding.UTF8.GetBytes("string 3") },
                ColDate = new SpannerDate(2020, 12, 28),
                ColDateArray = new List<SpannerDate?> { new SpannerDate(2020, 12, 28), new SpannerDate(2010, 1, 1), today },
                ColFloat64 = 3.14D,
                ColFloat64Array = new List<double?> { 3.14D, 6.626D },
                ColInt64 = id1,
                ColInt64Array = new List<long?> { 1L, 2L, 4L, 8L },
                ColJson = JsonDocument.Parse("{\"key\": \"value\"}"),
                ColJsonArray = new List<JsonDocument>{JsonDocument.Parse("{\"key1\": \"value1\"}"), null, JsonDocument.Parse("{\"key2\": \"value2\"}")},
                ColNumeric = (V1.SpannerNumeric?)3.14m,
                ColNumericArray = new List<V1.SpannerNumeric?> { (V1.SpannerNumeric)3.14m, (V1.SpannerNumeric)6.626m },
                ColString = "some string",
                ColStringArray = new List<string> { "string1", "string2", "string3" },
                ColStringMax = "some longer string",
                ColStringMaxArray = new List<string> { "longer string1", "longer string2", "longer string3" },
                ColTimestamp = new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288),
                ColTimestampArray = new List<DateTime?> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now },
            };
            var updateCount1 = await db.Database.ExecuteSqlRawAsync(@"INSERT INTO TableWithAllColumnTypes 
                              (ColBool, ColBoolArray, ColBytes, ColBytesMax, ColBytesArray, ColBytesMaxArray,
                               ColDate, ColDateArray, ColFloat64, ColFloat64Array, ColInt64, ColInt64Array,
                               ColJson, ColJsonArray, ColNumeric, ColNumericArray, ColString, ColStringArray,
                               ColStringMax, ColStringMaxArray, ColTimestamp, ColTimestampArray)
                              VALUES
                              (@ColBool, @ColBoolArray, @ColBytes, @ColBytesMax, @ColBytesArray, @ColBytesMaxArray,
                               @ColDate, @ColDateArray, @ColFloat64, @ColFloat64Array, @ColInt64, @ColInt64Array,
                               @ColJson, @ColJsonArray, @ColNumeric, @ColNumericArray, @ColString, @ColStringArray,
                               @ColStringMax, @ColStringMaxArray, @ColTimestamp, @ColTimestampArray)",
                new SpannerParameter("ColBool", SpannerDbType.Bool, row.ColBool),
                new SpannerParameter("ColBoolArray", SpannerDbType.ArrayOf(SpannerDbType.Bool), row.ColBoolArray),
                new SpannerParameter("ColBytes", SpannerDbType.Bytes, row.ColBytes),
                new SpannerParameter("ColBytesMax", SpannerDbType.Bytes, row.ColBytesMax),
                new SpannerParameter("ColBytesArray", SpannerDbType.ArrayOf(SpannerDbType.Bytes), row.ColBytesArray),
                new SpannerParameter("ColBytesMaxArray", SpannerDbType.ArrayOf(SpannerDbType.Bytes), row.ColBytesMaxArray),
                new SpannerParameter("ColDate", SpannerDbType.Date, row.ColDate),
                new SpannerParameter("ColDateArray", SpannerDbType.ArrayOf(SpannerDbType.Date), row.ColDateArray),
                new SpannerParameter("ColFloat64", SpannerDbType.Float64, row.ColFloat64),
                new SpannerParameter("ColFloat64Array", SpannerDbType.ArrayOf(SpannerDbType.Float64), row.ColFloat64Array),
                new SpannerParameter("ColInt64", SpannerDbType.Int64, row.ColInt64),
                new SpannerParameter("ColInt64Array", SpannerDbType.ArrayOf(SpannerDbType.Int64), row.ColInt64Array),
                new SpannerParameter("ColJson", db.IsEmulator ? SpannerDbType.String : SpannerDbType.Json, row.ColJson.RootElement.ToString()),
                new SpannerParameter("ColJsonArray", SpannerDbType.ArrayOf(db.IsEmulator ? SpannerDbType.String : SpannerDbType.Json), row.ColJsonArray.Select(v => v?.RootElement.ToString())),
                new SpannerParameter("ColNumeric", SpannerDbType.Numeric, row.ColNumeric),
                new SpannerParameter("ColNumericArray", SpannerDbType.ArrayOf(SpannerDbType.Numeric), row.ColNumericArray),
                new SpannerParameter("ColString", SpannerDbType.String, row.ColString),
                new SpannerParameter("ColStringArray", SpannerDbType.ArrayOf(SpannerDbType.String), row.ColStringArray),
                new SpannerParameter("ColStringMax", SpannerDbType.String, row.ColStringMax),
                new SpannerParameter("ColStringMaxArray", SpannerDbType.ArrayOf(SpannerDbType.String), row.ColStringMaxArray),
                new SpannerParameter("ColTimestamp", SpannerDbType.Timestamp, row.ColTimestamp),
                new SpannerParameter("ColTimestampArray", SpannerDbType.ArrayOf(SpannerDbType.Timestamp), row.ColTimestampArray)
            );
            Assert.Equal(1, updateCount1);

            var id2 = _fixture.RandomLong();
            row.ColInt64 = id2;
            var updateCount2 = await db.Database.ExecuteSqlInterpolatedAsync(
                @$"INSERT INTO TableWithAllColumnTypes 
                              (ColBool, ColBoolArray, ColBytes, ColBytesMax, ColBytesArray, ColBytesMaxArray,
                               ColDate, ColDateArray, ColFloat64, ColFloat64Array, ColInt64, ColInt64Array,
                               ColJson, ColJsonArray, ColNumeric, ColNumericArray, ColString, ColStringArray,
                               ColStringMax, ColStringMaxArray, ColTimestamp, ColTimestampArray)
                              VALUES
                              ({ row.ColBool}, { row.ColBoolArray}, { row.ColBytes}, { row.ColBytesMax}, { row.ColBytesArray}, { row.ColBytesMaxArray},
                               { row.ColDate}, { row.ColDateArray}, { row.ColFloat64}, { row.ColFloat64Array}, { row.ColInt64}, { row.ColInt64Array},
                               { (db.IsEmulator ? (object) row.ColJson.RootElement.ToString() : row.ColJson)},
                               { (db.IsEmulator ? (object) row.ColJsonArray.Select(v => v?.RootElement.ToString()).ToList() : row.ColJsonArray)},
                               { row.ColNumeric}, { row.ColNumericArray}, { row.ColString}, { row.ColStringArray},
                               { row.ColStringMax}, { row.ColStringMaxArray}, { row.ColTimestamp}, { row.ColTimestampArray})"
            );
            Assert.Equal(1, updateCount2);

            var id3 = _fixture.RandomLong();
            row.ColInt64 = id3;
            var updateCount3 = await db.Database.ExecuteSqlRawAsync(
                @"INSERT INTO TableWithAllColumnTypes 
                              (ColBool, ColBoolArray, ColBytes, ColBytesMax, ColBytesArray, ColBytesMaxArray,
                               ColDate, ColDateArray, ColFloat64, ColFloat64Array, ColInt64, ColInt64Array,
                               ColJson, ColJsonArray, ColNumeric, ColNumericArray, ColString, ColStringArray,
                               ColStringMax, ColStringMaxArray, ColTimestamp, ColTimestampArray)
                              VALUES
                              ({0}, {1}, {2}, {3}, {4}, {5},
                               {6}, {7}, {8}, {9}, {10}, {11},
                               {12}, {13}, {14}, {15}, {16}, {17},
                               {18}, {19}, {20}, {21})",
                               row.ColBool, row.ColBoolArray, row.ColBytes, row.ColBytesMax, row.ColBytesArray, row.ColBytesMaxArray,
                               row.ColDate, row.ColDateArray, row.ColFloat64, row.ColFloat64Array, row.ColInt64, row.ColInt64Array,
                               db.IsEmulator ? (object) row.ColJson.RootElement.ToString() : row.ColJson,
                               db.IsEmulator ? (object) row.ColJsonArray.Select(v => v?.RootElement.ToString()).ToList() : row.ColJsonArray,
                               row.ColNumeric, row.ColNumericArray, row.ColString, row.ColStringArray,
                               row.ColStringMax, row.ColStringMaxArray, row.ColTimestamp, row.ColTimestampArray
            );
            Assert.Equal(1, updateCount3);

            var rows = await db.TableWithAllColumnTypes
                .FromSqlRaw("SELECT * FROM TableWithAllColumnTypes WHERE ColInt64 IN UNNEST(@id)", new SpannerParameter("id", SpannerDbType.ArrayOf(SpannerDbType.Int64), new List<long> { id1, id2, id3 }))
                .OrderBy(s => s.ColString)
                .ToListAsync();
            Assert.Collection(rows,
                row => Assert.NotNull(row.ColDateArray),
                row => Assert.NotNull(row.ColDateArray),
                row => Assert.NotNull(row.ColDateArray)
            );
        }

        [Fact]
        public async Task CanQueryOnReservedKeywords()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id1 = _fixture.RandomLong();
            var id2 = _fixture.RandomLong();
            db.TableWithAllColumnTypes.Add(
               new TableWithAllColumnTypes
               {
                   ColInt64 = id1,
                   ASC = "This is reserved keyword"
               });
            await db.SaveChangesAsync();
            db.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes
            {
                ColInt64 = id2,
                ASC = "string1"
            });
            await db.SaveChangesAsync();

            // Select query
            var result = db.TableWithAllColumnTypes
                .Where(s => new long[] { id1, id2 }.Contains(s.ColInt64))
                .OrderBy(s => s.ASC)
                .Select(c => c.ASC).ToList();
            Assert.Collection(result,
                s => Assert.Equal("This is reserved keyword", s),
                s => Assert.Equal("string1", s));

            // Where clause
            var result1 = db.TableWithAllColumnTypes
                .Where(s => new long[] { id1, id2 }.Contains(s.ColInt64))
                .Where(s => s.ASC == "string1")
                .Select(c => c.ASC).ToList();
            Assert.Collection(result1, s => Assert.Equal("string1", s));

            // Start with query
            var result2 = db.TableWithAllColumnTypes
                .Where(s => new long[] { id1, id2 }.Contains(s.ColInt64))
                .Where(s => s.ASC.StartsWith("This"))
                .Select(c => c.ASC).ToList();
            Assert.Collection(result2, s => Assert.Equal("This is reserved keyword", s));

            // Contain query
            var result3 = db.TableWithAllColumnTypes
                .Where(s => new long[] { id1, id2 }.Contains(s.ColInt64))
                .Where(s => s.ASC.Contains("1"))
                .Select(c => c.ASC).ToList();
            Assert.Collection(result3, s => Assert.Equal("string1", s));

            // Like function
            var result4 = db.TableWithAllColumnTypes
                .Where(s => new long[] { id1, id2 }.Contains(s.ColInt64))
                .Where(s => EF.Functions.Like(s.ASC, "%1"))
                .Select(c => c.ASC).ToList();
            Assert.Collection(result4, s => Assert.Equal("string1", s));
        }
        */
    }
}
