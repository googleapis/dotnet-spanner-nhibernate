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
using NHibernate;
using NHibernate.Criterion;
using NHibernate.Exceptions;
using NHibernate.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests
{
    [Collection(nameof(NonParallelTestCollection))]
    public class TransactionTests : IClassFixture<SpannerSampleFixture>
    {
        private readonly SpannerSampleFixture _fixture;

        public TransactionTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [InlineData(MutationUsage.Never)]
        [InlineData(MutationUsage.Always)]
        [Theory]
        public async Task SaveChangesIsAtomic(MutationUsage mutationUsage)
        {
            var invalidSingerId = Guid.NewGuid().ToString();
            var sessionFactory = mutationUsage == MutationUsage.Always
                ? _fixture.SessionFactoryForMutations
                : _fixture.SessionFactory;
            string singerId;
            using (var session = sessionFactory.OpenSession())
            {
                var tx = session.BeginTransaction(mutationUsage);
                // Try to add a singer and an album in one transaction.
                // The album is invalid. Both the singer and the album
                // should not be inserted.
                singerId = (string) await session.SaveAsync(new Singer
                {
                    FirstName = "Joe",
                    LastName = "Elliot",
                });
                await session.SaveAsync(new Album
                {
                    Singer = new Singer{ Id = invalidSingerId }, // Invalid, does not reference an actual Singer
                    Title = "Some title",
                });
                // DML will fail during the update, which causes a SpannerException.
                // Mutations will fail during the commit, which will cause an NHibernate.TransactionException.
                var expectedException = mutationUsage == MutationUsage.Always
                    ? typeof(TransactionException)
                    : typeof(SpannerException);
                await Assert.ThrowsAsync(expectedException, () => tx.CommitAsync());
            }

            using (var session = _fixture.SessionFactory.OpenSession())
            {
                // Verify that the singer was not inserted in the database.
                Assert.Null(await session.GetAsync<Singer>(singerId));
            }
        }

        [InlineData(MutationUsage.Never)]
        [InlineData(MutationUsage.Always)]
        [Theory]
        public async Task EndOfTransactionScopeCausesRollback(MutationUsage mutationUsage)
        {
            var venueCode = _fixture.RandomString(4);
            using var session = _fixture.SessionFactory.OpenSession();
            using (var unused = session.BeginTransaction(mutationUsage))
            {
                await session.SaveAsync(new Venue
                {
                    Code = venueCode,
                    Name = "Venue 3",
                });
                await session.FlushAsync();
                // End the transaction scope without any explicit rollback.
            }
            // Verify that the venue was not inserted.
            var venuesAfterRollback = session.Query<Venue>()
                .Where(v => v.Code == venueCode)
                .ToList();
            Assert.Empty(venuesAfterRollback);
        }

        [InlineData(MutationUsage.Never)]
        [InlineData(MutationUsage.Always)]
        [Theory]
        public async Task TransactionCanReadYourWrites(MutationUsage mutationUsage)
        {
            var venueCode1 = _fixture.RandomString(4);
            var venueCode2 = _fixture.RandomString(4);
            using var session = _fixture.SessionFactory.OpenSession();

            using var transaction = session.BeginTransaction(mutationUsage);
            // Add two venues in the transaction.
            await session.SaveAsync(new Venue
            {
                Code = venueCode1,
                Name = "Venue 1",
            });
            await session.SaveAsync(new Venue
            {
                Code = venueCode2,
                Name = "Venue 2",
            });
            await session.FlushAsync();

            // Verify that we can read the venue while inside the transaction if we are using DML.
            if (mutationUsage != MutationUsage.Always)
            {
                var venues = session.Query<Venue>()
                    .Where(v => v.Code == venueCode1 || v.Code == venueCode2)
                    .OrderBy(v => v.Name)
                    .ToList();
                Assert.Equal(2, venues.Count);
                Assert.Equal("Venue 1", venues[0].Name);
                Assert.Equal("Venue 2", venues[1].Name);
            }
            else
            {
                // Read-your-writes is not supported if we are using mutations.
                var venues = session.Query<Venue>()
                    .Where(v => v.Code == venueCode1 || v.Name == venueCode2)
                    .ToList();
                Assert.Empty(venues);
            }
            // Rollback and then verify that we should not be able to see the venues.
            await transaction.RollbackAsync();

            // Verify that the venues can no longer be read.
            var sessionAfterRollback = _fixture.SessionFactory.OpenSession();
            var venuesAfterRollback = sessionAfterRollback.Query<Venue>()
                .Where(v => v.Code == venueCode1 || v.Name == venueCode2)
                .ToList();
            Assert.Empty(venuesAfterRollback);
        }

        [InlineData(MutationUsage.Never)]
        [InlineData(MutationUsage.Always)]
        [Theory]
        public async Task TransactionCanReadCommitTimestamp(MutationUsage mutationUsage)
        {
            var id = _fixture.RandomLong();
            using var session = _fixture.SessionFactory.OpenSession();

            using var transaction = session.BeginTransaction(mutationUsage);
            // Add a row that will generate a commit timestamp.
            var row = new TableWithAllColumnTypes { ColInt64 = id };
            await session.SaveAsync(row);
            await session.FlushAsync();
            // The transaction has not yet been committed, so there is still
            // no commit timestamp available.
            Assert.Null(row.ColCommitTs);

            // Columns that have a pending commit timestamp cannot be read.
            // https://cloud.google.com/spanner/docs/commit-timestamp#dml
            // This also means that we cannot mark the commit timestamp column
            // as a column that has a generated value, as that would trigger a
            // result propagation during the same transaction.
            if (mutationUsage != MutationUsage.Always)
            {
                var exception = await Assert.ThrowsAsync<GenericADOException>(() =>
                    session
                        .Query<TableWithAllColumnTypes>()
                        .Where(r => r.ColInt64 == id)
                        .Select(r => new { r.ColInt64, r.ColCommitTs })
                        .FirstOrDefaultAsync());
                Assert.Contains("has a pending CommitTimestamp",
                    exception.InnerException?.InnerException?.Message ?? "");
            }
            // Commit the transaction. This will generate a commit timestamp.
            await transaction.CommitAsync();
            // TODO: Use an interceptor to propagate the commit timestamp to the entity.
            // The commit timestamp is not automatically propagated to the entity after the commit.
            Assert.Null(row.ColCommitTs);

            // If we read the row back through the same session using the primary key value,
            // we will get the cached object. The commit timestamp has not been automatically propagated
            // by the SpannerRetriableTransaction to the entity.
            var rowUpdated = await session.LoadAsync<TableWithAllColumnTypes>(id);
            Assert.NotNull(rowUpdated);
            Assert.Null(rowUpdated.ColCommitTs);

            // Detaching the entity from the context and re-getting it should give us the commit timestamp.
            await session.EvictAsync(rowUpdated);
            var rowRefreshed = await session.LoadAsync<TableWithAllColumnTypes>(id);
            Assert.NotNull(rowRefreshed);
            Assert.NotNull(rowRefreshed.ColCommitTs);
        }

        [Fact]
        public async Task ImplicitTransactionCanReadCommitTimestamp()
        {
            var id = _fixture.RandomLong();
            using var session = _fixture.SessionFactory.OpenSession();

            // Add a row that will generate a commit timestamp.
            var row = new TableWithAllColumnTypes { ColInt64 = id };
            await session.SaveAsync(row);
            Assert.Null(row.ColCommitTs);
            await session.FlushAsync();
            // TODO: Automatically propagate commit timestamps to the entity after commit.
            Assert.Null(row.ColCommitTs);

            // If we read the row back through the same database context using the primary key value,
            // we will get the cached object. The commit timestamp has not been automatically propagated
            // by the SpannerRetriableTransaction to the entity.
            var rowUpdated = await session.LoadAsync<TableWithAllColumnTypes>(id);
            Assert.NotNull(rowUpdated);
            Assert.Null(rowUpdated.ColCommitTs);

            // Detaching the entity from the context and re-getting it should give us the commit timestamp.
            await session.EvictAsync(rowUpdated);
            var rowRefreshed = await session.LoadAsync<TableWithAllColumnTypes>(id);
            Assert.NotNull(rowRefreshed);
            Assert.NotNull(rowRefreshed.ColCommitTs);
        }

        [Fact]
        public async Task CanUseReadOnlyTransaction()
        {
            var venueCode = _fixture.RandomString(4);
            var venueName = _fixture.RandomString(10);

            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            // Add a venue.
            await session.SaveAsync(new Venue
            {
                Code = venueCode,
                Name = venueName,
            });
            var dbTransaction = transaction.GetDbTransaction();
            await transaction.CommitAsync();
            var commitTimestamp = dbTransaction.CommitTimestamp;

            // Try to read the venue using a read-only transaction.
            using var readOnlyTransaction = session.BeginReadOnlyTransaction();
            var foundVenue = await session.Query<Venue>().Where(v => v.Code == venueCode).FirstOrDefaultAsync();
            Assert.NotNull(foundVenue);
            Assert.Equal(venueName, foundVenue.Name);
            // Read-only transactions cannot really be committed, but this releases the resources
            // that are used by the transaction and enables us to start a new transaction on the session.
            await readOnlyTransaction.CommitAsync();

            // Try to read the venue using a read-only transaction that reads using
            // a timestamp before the above venue was added. It should not return any results.
            using var readOnlyTransactionBeforeAdd = session.BeginReadOnlyTransaction(TimestampBound.OfReadTimestamp(commitTimestamp.AddMilliseconds(-1)));
            var result = await session.Query<Venue>().Where(v => v.Code == venueCode).FirstOrDefaultAsync();
            Assert.Null(result);
        }

        [Fact]
        public async Task CanExecuteStaleRead()
        {
            var venueCode = _fixture.RandomString(4);
            var venueName = _fixture.RandomString(10);
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();

            // Add a venue.
            await session.SaveAsync(new Venue
            {
                Code = venueCode,
                Name = venueName,
            });
            var dbTransaction = transaction.GetDbTransaction();
            await transaction.CommitAsync();
            var commitTimestamp = dbTransaction.CommitTimestamp;

            // Try to read the venue using a single use read-only transaction with strong timestamp bound.
            var foundVenue = await session
                .WithTimestampBound(TimestampBound.Strong)
                .Query<Venue>()
                .Where(v => v.Code == venueCode).FirstOrDefaultAsync();
            Assert.NotNull(foundVenue);
            Assert.Equal(venueName, foundVenue.Name);

            // Try to read the venue using a single use read-only transaction that reads using
            // a timestamp before the above venue was added. It should not return any results.
            var result = await session
                .WithTimestampBound(TimestampBound.OfReadTimestamp(commitTimestamp.AddMilliseconds(-1)))
                .Query<Venue>()
                .Where(v => v.Code == venueCode).FirstOrDefaultAsync();
            Assert.Null(result);
            
            // Also try to read the venue using a single use read-only transaction that reads
            // using a max staleness. Note that this could cause a `Table not found` exception if
            // the read timestamp that is chosen by the backend is before the table was created.
            try
            {
                result = await session
                    .WithTimestampBound(TimestampBound.OfMaxStaleness(TimeSpan.FromSeconds(1)))
                    .Query<Venue>()
                    .Where(v => v.Code == venueCode).FirstOrDefaultAsync();
                // The read timestamp is chosen by the backend and could be before or after the venue was created.
                if (result != null)
                {
                    Assert.Equal(venueName, result.Name);
                }
            }
            catch (Exception e)
            {
                Assert.Contains("Table not found", e.Message);
            }
        }

        [Fact]
        public async Task CanUseComputedColumnAndCommitTimestamp()
        {
            var id1 = _fixture.RandomLong();
            var id2 = _fixture.RandomLong();

            // TODO: Modify this test case to also use an explicit transactions.
            // TODO: Modify this test case to also use mutations.
            using var session = _fixture.SessionFactory.OpenSession();
            await session.SaveAsync(new TableWithAllColumnTypes
                { ColInt64 = id1, ColStringArray = new SpannerStringArray(new List<string> { "1", "2", "3" }) });
            await session.SaveAsync(new TableWithAllColumnTypes
                { ColInt64 = id2, ColStringArray = new SpannerStringArray(new List<string> { "4", "5", "6" }) });
            await session.FlushAsync();

            var rows = await session.Query<TableWithAllColumnTypes>()
                .Where(row => new[] { id1, id2 }.Contains(row.ColInt64))
                .OrderBy(row => row.ColInt64 == id1 ? 1 : 2) // This ensures that the row with id1 is returned as the first result.
                .ToListAsync();
            Assert.Collection(rows,
                row => Assert.Equal("1,2,3", row.ColComputed),
                row => Assert.Equal("4,5,6", row.ColComputed)
            );
            // The rows were inserted in the same transaction and should therefore have the same commit timestamp.
            Assert.Equal(rows[0].ColCommitTs, rows[1].ColCommitTs);
        }

        [SkippableTheory]
        [CombinatorialData]
        public void TransactionRetry(bool disableInternalRetries)
        {
            Skip.If(SpannerFixtureBase.IsEmulator, "Emulator does not support multiple simultaneous transactions");
            const int transactions = 8;
            var aborted = new List<Exception>();
            Parallel.For(0L, transactions, (i, state) =>
            {
                try
                {
                    // The internal retry mechanism should be able to catch and retry
                    // all aborted transactions. If internal retries are disabled, multiple
                    // transactions will abort.
                    InsertRandomSinger(disableInternalRetries).Wait();
                }
                catch (AggregateException e) when (e.InnerException is SpannerException se && se.ErrorCode == ErrorCode.Aborted)
                {
                    lock (aborted)
                    {
                        aborted.Add(se);
                    }
                    // We don't care exactly how many transactions were aborted, only whether
                    // at least one or none was aborted.
                    state.Stop();
                }
            });
            Assert.True(
                disableInternalRetries == (aborted.Count > 0),
                $"Unexpected aborted count {aborted.Count} for disableInternalRetries={disableInternalRetries}. First aborted error: {aborted.FirstOrDefault()?.Message ?? "<none>"}"
            );
        }

        [InlineData(MutationUsage.Never)]
        [InlineData(MutationUsage.Always)]
        [Theory]
        public async Task ComputedColumnIsPropagatedInManualTransaction(MutationUsage mutationUsage)
        {
            var sessionFactory = mutationUsage == MutationUsage.Always
                ? _fixture.SessionFactoryForMutations
                : _fixture.SessionFactory;
            using var session = sessionFactory.OpenSession();
            using var transaction = session.BeginTransaction(mutationUsage);
            var singer = new Singer
            {
                FirstName = "Alice",
                LastName = "Ferguson",
            };
            var id  = await session.SaveAsync(singer);
            
            // A flush has no effect for a transaction that uses mutations.
            await session.FlushAsync();

            if (mutationUsage == MutationUsage.Always)
            {
                Assert.Null(singer.FullName);
            }
            else
            {
                Assert.Equal("Alice Ferguson", singer.FullName);
            }

            await transaction.CommitAsync();
            // The value is readable after a commit for both types of transactions, but only if a reload is forced.
            await session.EvictAsync(singer);
            var row = await session.LoadAsync<Singer>(id);
            Assert.Equal("Alice Ferguson", row.FullName);
        }
        
        private async Task InsertRandomSinger(bool disableInternalRetries)
        {
            var rnd = new Random(Guid.NewGuid().GetHashCode());
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            if (disableInternalRetries)
            {
                transaction.DisableInternalRetries();
            }

            var rows = rnd.Next(1, 10);
            for (var row = 0; row < rows; row++)
            {
                // This test assumes that this is random enough and that the id's
                // will never overlap during a test run.
                var id = _fixture.RandomLong(rnd);
                var prefix = id.ToString("D20");
                // First name is required, so we just assign a meaningless random value.
                var firstName = "FirstName" + "-" + rnd.Next(10000).ToString("D4");
                // Last name contains the same value as the primary key with a random suffix.
                // This makes it possible to search for a singer using the last name and knowing
                // that the search will at most deliver one row (and it will be the same row each time).
                var lastName = prefix + "-" + rnd.Next(10000).ToString("D4");

                // Yes, this is highly inefficient, but that is intentional. This
                // will cause a large number of the transactions to be aborted.
                var existing = await session
                    .Query<Singer>()
                    .Where(v => v.LastName.IsLike(prefix, MatchMode.Start))
                    .OrderBy(v => v.LastName)
                    .FirstOrDefaultAsync();

                if (existing == null)
                {
                    await session.SaveAsync(new Singer
                    {
                        FirstName = firstName,
                        LastName = lastName,
                    });
                }
                else
                {
                    existing.FirstName = firstName;
                    await session.UpdateAsync(existing);
                }
                await session.FlushAsync();
            }
            await transaction.CommitAsync();
        }
    }
}
