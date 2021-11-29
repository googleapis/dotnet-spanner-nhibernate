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

using Google.Cloud.Spanner.Connection.MockServer;
using Google.Cloud.Spanner.NHibernate.Tests.Entities;
using Grpc.Core;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Mapping.ByCode;
using NHibernate.SqlCommand;
using NHibernate.Util;
using System;
using Environment = NHibernate.Cfg.Environment;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    // ReSharper disable once ClassNeverInstantiated.Global
    internal class TestConnectionProvider : SpannerConnectionProvider
    {
        public override ChannelCredentials ChannelCredentials { get => ChannelCredentials.Insecure; set => throw new InvalidOperationException(); }
    }
    
    public class NHibernateMockServerFixture : SpannerMockServerFixture
    {
        public NHibernateMockServerFixture()
        {
            ReflectHelper.ClassForName(typeof(SpannerDriver).AssemblyQualifiedName);
            var nhConfig = new Configuration().DataBaseIntegration(db =>
            {
                db.Dialect<SpannerDialect>();
                db.ConnectionString = ConnectionString;
                db.BatchSize = 100;
                db.ConnectionProvider<TestConnectionProvider>();
            });
            var mapper = new ModelMapper();
            mapper.AddMapping<SingerMapping>();
            mapper.AddMapping<AlbumMapping>();
            mapper.AddMapping<TableWithAllColumnTypesMapping>();
            mapper.AddMapping<TrackMapping>();
            var mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            nhConfig.AddMapping(mapping);
            
            SessionFactory = nhConfig.BuildSessionFactory();
                        
            // This is needed for support for query hints.
            nhConfig.SetInterceptor(new SpannerQueryHintInterceptor());
            nhConfig.Properties[Environment.UseSqlComments] = "true";
            SessionFactoryWithComments = nhConfig.BuildSessionFactory();

            // Configure some entities to use mutations instead of DML in a separate SessionFactory.
            nhConfig.GetClassMapping(typeof(Album)).EntityPersisterClass = typeof(SpannerMutationsEntityPersister);
            nhConfig.GetClassMapping(typeof(Album)).DynamicUpdate = true;
            nhConfig.GetClassMapping(typeof(Track)).EntityPersisterClass = typeof(SpannerMutationsEntityPersister);
            nhConfig.GetClassMapping(typeof(Track)).DynamicUpdate = true;
            SessionFactoryUsingMutations = nhConfig.BuildSessionFactory();
        }

        public ISessionFactory SessionFactory { get; }
        
        public ISessionFactory SessionFactoryWithComments { get; }
        
        public ISessionFactory SessionFactoryUsingMutations { get; }
        
        public string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={Host};Port={Port}";
    }
}