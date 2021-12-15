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
using NHibernate;
using NHibernate.Linq;
using NHibernate.Mapping.ByCode;
using System;
using System.Linq;
using System.Threading.Tasks;
using Environment = NHibernate.Cfg.Environment;
using Mapping = NHibernate.Mapping;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// This sample shows how to use Mutations instead of DML with the Cloud Spanner NHibernate
    /// driver and dialect. It is recommended to read https://cloud.google.com/spanner/docs/dml-versus-mutations
    /// if you are not familiar with the differences between Mutations and DML in Cloud Spanner.
    /// 
    /// By default, the Cloud Spanner NHibernate driver will use DML for all insert/update/delete
    /// operations. The reason for this default behavior is that:
    /// 1. NHibernate recommends that applications should always use transactions when modifying
    ///    data. DML supports read-your-writes during transactions, while Mutations do not.
    /// 2. Mutations do not support arbitrary WHERE clauses in updates. Instead, they require
    ///    that a value is provided for each primary key column in the table. This is not compatible
    ///    with all update statements that are generated for mapped collections, as these sometimes
    ///    update multiple collection elements in one statement.
    ///
    /// It can however be more efficient to use Mutations instead of DML for certain use cases.
    /// Especially bulk insert/update/delete operations that are executed using NHibernate can
    /// profit from using Mutations instead of DML. It is however in that case important to realize
    /// that certain limitations apply:
    /// 1. Read-your-writes during a transaction is never possible. Manually flushing the NHibernate
    ///    session during a transaction will not send any mutations to Cloud Spanner. Mutations are
    ///    only sent to Cloud Spanner when the transaction is committed.
    /// 2. Updates to mapped collections that generate statements that update multiple rows will be
    ///    executed using DML and not Mutations. It is however generally advisable not to use
    ///    collection mappings that are automatically managed by NHibernate.
    /// 3. Entities with generated columns cannot be used in combination with Mutations, except if
    ///    the column generation strategy is (temporarily) set to <see cref="PropertyGeneration.Never"/>.
    ///    The reason that generated columns cannot be used with Mutations ,is that NHibernate will
    ///    try to read the generated value from the database directly after an entity has been
    ///    updated. This read operation will fail as Mutations do not support read-your-writes.
    ///
    /// Mutations can only be used in combination with batching. It is therefore required that
    /// batching is enabled in the NHibernate configuration. Also, NHibernate will by default not
    /// batch versioned data. That means that any UPDATE or DELETE statement on entities that are
    /// versioned will by default not use mutations. Enable the setting BatchVersionedData in your
    /// NHibernate configuration to use mutations for UPDATES and DELETES. To summarize:
    /// 1. Set BatchSize>=1 in your NHibernate configuration.
    /// 2. Set BatchVersionedData=true in your NHibernate configuration.
    ///
    /// An application can configure a <see cref="ISession"/> to use Mutations instead of DML for
    /// all transactions, or for all data modifications using an implicit transaction. It is also
    /// possible to start a single transaction that will use mutations, regardless of the session
    /// configuration.
    /// 
    /// Run from the command line with `dotnet run Mutations`
    /// </summary>
    public static class MutationsSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            // Mutations can only be used if batching has been enabled in the NHibernate configuration.
            if (!configuration.Configuration.Properties.TryGetValue(Environment.BatchSize, out var batchSize)
                || int.Parse(batchSize) < 1)
            {
                throw new ArgumentException("Mutations can only be used if batching has been enabled. Set a batch size of at least 1.");
            }
            if (!configuration.Configuration.Properties.TryGetValue(Environment.BatchVersionedData, out var batchVersionedData)
                || !bool.Parse(batchVersionedData))
            {
                throw new ArgumentException("Mutations can only be used for updates and deletes if batching has been enabled for versioned data. " +
                                            "Set BatchVersionedData to true.");
            }
            
            // Mutations cannot be used in combination with generated columns. If your application has entities with
            // generated columns, you need to set the value generation to Never and instruct NHibernate to not insert
            // or update the value in the column. Cloud Spanner will still generate a value for the column, but the
            // generated value will not be available in NHibernate before you manually refresh the entity.
            var singerMapping = configuration.Configuration.GetClassMapping(typeof(Singer));
            var fullName = singerMapping.GetProperty(nameof(Singer.FullName));
            fullName.Generation = Mapping.PropertyGeneration.Never;
            fullName.IsInsertable = false;
            fullName.IsUpdateable = false;
                
            // Create a SessionFactory that does not use a generated value for Singer.FullName.
            var sessionFactory = configuration.Configuration.BuildSessionFactory();
            
            // Create a session that will always use mutations.
            using var sessionUsingMutations = sessionFactory.OpenSession().SetBatchMutationUsage(MutationUsage.Always);

            // Create a new Singer and save it. The insert will use a Mutation, and the new entity will not be readable
            // until the transaction has been committed.
            var transaction = sessionUsingMutations.BeginTransaction();
            var singer = new Singer
            {
                FirstName = "Wanda",
                LastName = "Yates",
            };
            // The id is generated in the client, and is therefore directly available, even if the mutation itself is
            // not yet sent to Cloud Spanner.
            var singerId = await sessionUsingMutations.SaveAsync(singer);
            await sessionUsingMutations.FlushAsync();

            // Try to get all singers. This will return zero results, as the mutation will only be sent to Cloud Spanner
            // once the transaction has been committed.
            var allSingers = await sessionUsingMutations.Query<Singer>().ToListAsync();
            Console.WriteLine($"Number of singers found BEFORE committing the transaction: {allSingers.Count}");
            // Commit the transaction and try to load all singers again.
            await transaction.CommitAsync();
            allSingers = await sessionUsingMutations.Query<Singer>().ToListAsync();
            Console.WriteLine($"Number of singers found AFTER committing the transaction: {allSingers.Count}");
            Console.WriteLine("");

            // The FullName property of the singer will not automatically be populated.
            Console.WriteLine($"Full name of the singer (BEFORE refreshing): {singer.FullName}");
            // However, if we refresh the entity, we will see that Cloud Spanner still generated a value for it.
            await sessionUsingMutations.RefreshAsync(singer);
            Console.WriteLine($"Full name of the singer (AFTER refreshing): {singer.FullName}");
            Console.WriteLine("");
            sessionUsingMutations.Close();

            // Mutation usage can also be specified for a specific transaction. The following session will by default
            // use DML for all data modifications, unless we start a transaction that specifically should use mutations.
            using var session = sessionFactory.OpenSession();
            using var transactionWithMutations = session.BeginTransaction(MutationUsage.Always);
            singer = await session.LoadAsync<Singer>(singerId);
            singer.LastName = "Yates-Fish";
            
            // Manually flush the session. This would normally send a DML statement to Cloud Spanner, but a Mutation is
            // not sent until the transaction is committed.
            await session.FlushAsync();
            var singersWithLastNameYatesFish =
                await session.Query<Singer>().Where(s => s.LastName.Equals("Yates-Fish")).ToListAsync();
            Console.WriteLine($"Singers with LastName 'Yates-Fish' BEFORE committing: {singersWithLastNameYatesFish.Count}");

            // Committing the transaction will send the Mutation to Cloud Spanner and the updated value should be visible.
            await transactionWithMutations.CommitAsync();
            singersWithLastNameYatesFish =
                await session.Query<Singer>().Where(s => s.LastName.Equals("Yates-Fish")).ToListAsync();
            Console.WriteLine($"Singers with LastName 'Yates-Fish' AFTER committing: {singersWithLastNameYatesFish.Count}");
            Console.WriteLine("");
        }
    }
}