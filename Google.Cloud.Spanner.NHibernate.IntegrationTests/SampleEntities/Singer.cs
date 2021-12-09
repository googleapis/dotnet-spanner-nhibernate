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

using NHibernate.Id;
using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;
using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests.SampleEntities
{
    public class Singer
    {
        public Singer()
        {
        }

        public virtual string Id { get; set; }
        public virtual string FirstName { get; set; }
        public virtual string LastName { get; set; }
        public virtual string FullName { get; set; }
        public virtual SpannerDate BirthDate { get; set; }
        public virtual byte[] Picture { get; set; }

        public virtual IList<Album> Albums { get; set; }
        public virtual IList<Concert> Concerts { get; set; }
    }

    public class SingerMapping : ClassMapping<Singer>
    {
        public SingerMapping()
        {
            Persister<SpannerSingleTableEntityPersister>();
            DynamicUpdate(true);
            Table("Singers");
            Id(x => x.Id, m =>
            {
                m.Generator(new UUIDHexGeneratorDef());
                m.Length(36);
            });
            Property(x => x.FirstName, m => m.Length(200));
            Property(x => x.LastName, m =>
            {
                m.NotNullable(true);
                m.Length(200);
            });
            Property(x => x.FullName, m =>
            {
                m.NotNullable(true);
                m.Length(400);
                m.Generated(PropertyGeneration.Always);
                m.Index("Idx_Singers_FullName");
            });
            Property(x => x.BirthDate);
            Property(x => x.Picture);
            Bag(x => x.Albums, c => { }, r => r.OneToMany());
            Bag(x => x.Concerts, c => { }, r => r.OneToMany());
        }
    }
}
