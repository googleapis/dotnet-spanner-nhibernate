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
using NHibernate.Cfg;
using NHibernate.Mapping.ByCode;

namespace Google.Cloud.Spanner.NHibernate.Samples
{
    /// <summary>
    /// Configuration for the Cloud Spanner NHibernate samples. This configuration will build a
    /// <see cref="SessionFactory"/> that will connect to a local Spanner emulator.
    ///
    /// All NHibernate mapping is done in code.
    /// </summary>
    public class SampleConfiguration
    {
        public ISessionFactory SessionFactory { get; }
        
        public Configuration Configuration { get; }

        public SampleConfiguration(string connectionString)
        {
            Configuration = new Configuration().DataBaseIntegration(db =>
            {
                db.Dialect<SpannerDialect>();
                db.ConnectionString = connectionString;
                db.BatchSize = 100;
            });
            var mapper = new ModelMapper();
            
            // The order that mapped classes are added to the configuration is for most tables not important, except for
            // interleaved tables. Interleaved tables must be added in the order parent-child. In this case, that means
            // that Album *MUST* be added before Track, as Track is marked as `INTERLEAVE IN PARENT Albums`.
            mapper.AddMapping<SingerMapping>();
            mapper.AddMapping<AlbumMapping>();
            mapper.AddMapping<TrackMapping>();
            
            mapper.AddMapping<BandMapping>();
            mapper.AddMapping<BandMembershipMapping>();
            
            mapper.AddMapping<VenueMapping>();
            mapper.AddMapping<ConcertMapping>();
            mapper.AddMapping<PerformanceMapping>();
            
            var mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            Configuration.AddMapping(mapping);
            
            // Both the following lines are needed for support for query hints.
            Configuration.SetInterceptor(new SpannerQueryHintInterceptor());
            Configuration.Properties[Environment.UseSqlComments] = "true";
            
            // The following enables batching UPDATE and DELETE statements for entities that are versioned.
            Configuration.Properties[Environment.BatchVersionedData] = "true";
            
            SessionFactory = Configuration.BuildSessionFactory();
        }
    }
}