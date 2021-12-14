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
    /// Cloud Spanner supports generated columns: https://cloud.google.com/spanner/docs/generated-column/how-to
    /// These properties must be mapped as `Generated always` in the NHibernate mapping. An example of this can be found
    /// for the <see cref="Singer.FullName"/> property in <see cref="SingerMapping"/>.
    ///
    /// Note that annotating a property as generated will cause NHibernate to execute a SELECT after EACH insert/update
    /// statement for the entity that contains the generated property. This can have a negative impact on performance.
    /// It is also possible to mark a generated column in Cloud Spanner as insert=false and update=false. This will
    /// prevent NHibernate from generating insert/update statements that include the generated column. The value will
    /// automatically be updated by Cloud Spanner, and the value can be read back manually by refreshing the entity when
    /// that is necessary.
    ///
    /// See also https://nhibernate.info/doc/nhibernate-reference/mapping.html#mapping-generated
    /// 
    /// Run from the command line with `dotnet run GeneratedColumnSample`
    /// </summary>
    public static class GeneratedColumnSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();
            using var transaction1 = session.BeginTransaction();

            // Singer has a generated column FullName that is the combination of the
            // FirstName and LastName. The value is automatically computed by Cloud Spanner.
            // Setting it manually client side has no effect.
            var singer = new Singer
            {
                FirstName = "Alice",
                LastName = "Jameson"
            };
            await session.SaveAsync(singer);
            await transaction1.CommitAsync();

            // Entity Framework will automatically fetch the computed value for FullName
            // from Cloud Spanner after it has been written.
            Console.WriteLine($"Added singer with full name {singer.FullName}");

            // Updating the last name of the singer will also update the full name.
            using var transaction2 = session.BeginTransaction();
            singer.LastName = "Jameson - Cooper";
            await transaction2.CommitAsync();
            
            Console.WriteLine($"Updated singer's full name to {singer.FullName}");
        }
    }
}