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
    /// This sample shows the recommended ways to manage collection mappings when using the Cloud Spanner NHibernate
    /// dialect and driver.
    ///
    /// NHibernate allows mapping collections in a number of different ways, and while all collection mappings are
    /// supported for Cloud Spanner, some mappings will generate a large number of DML statements that will remove and
    /// re-inserts relationships between entities. This especially applies to many-to-many relationships. These types of
    /// collection mappings are not recommended when working with Cloud Spanner. Instead, a many-to-many relationship
    /// should be mapped using a relationship entity.
    ///
    /// Follow these recommendations whenever possible when working with mapped collections in NHibernate:
    /// 1. Do not use many-to-many relationships. Instead, model these using a relationship entity between the two
    ///    entities that have a many-to-many relationship. See the Singer-BandMembership-Band example in this sample.
    /// 2. Always mark the one-to-many side of a mapped collection as Inverse(true). This will make the child side of
    ///    the collection mapping the owner of the collection, which again will reduce the number of DML statements that
    ///    are needed to maintain the collection.
    /// 
    /// Run from the command line with `dotnet run CollectionMapping`
    /// </summary>
    public static class CollectionMappingSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            Console.WriteLine("Starting collection mapping sample");
            using var session = configuration.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();

            // Create a Singer and a Band and create a relationship between the two.
            var singer = new Singer
            {
                FirstName = "Carolina",
                LastName = "Schofield",
            };
            var band = new Band
            {
                Name = "The Pointless Cats",
            };
            var bandMembership = new BandMembership
            {
                Band = band,
                Singer = singer,
                BeginDate = DateTime.Now,
            };
            // Saving these three entities will be done in three DML statements (or three mutations).
            // NHibernate will not generate any additional DML statements in order to update any collections.
            await session.SaveAsync(singer);
            await session.SaveAsync(band);
            await session.SaveAsync(bandMembership);
            await transaction.CommitAsync();
            Console.WriteLine("Saved singer, band and band membership");
            Console.WriteLine();

            // Refreshing the entities will allow us to get the bands of a singer through the mapped collection.
            await session.RefreshAsync(singer);
            Console.WriteLine($"Singer {singer.FullName} is a member of:");
            foreach (var membership in singer.BandMemberships)
            {
                Console.WriteLine(
                    $"\t{membership.Band.Name} ({membership.BeginDate} - {membership.EndDate?.ToString() ?? "..."})");
            }
        }
    }
}