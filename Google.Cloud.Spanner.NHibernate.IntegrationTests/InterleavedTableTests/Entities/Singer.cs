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

using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;
using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests.InterleavedTableTests.Entities
{
    public class Singer
    {
        public Singer()
        {
        }

        public virtual string SingerId { get; set; }
        public virtual string FirstName { get; set; }
        public virtual string LastName { get; set; }
        public virtual IList<Album> Albums { get; set; }
    }

    public class SingerMapping : ClassMapping<Singer>
    {
        public SingerMapping()
        {
            Persister<SpannerSingleTableEntityPersister>();
            Table("Singers");
            Id(x => x.SingerId, m =>
            {
                m.Generator(new UUIDHexGeneratorDef());
                m.Length(36);
            });
            Property(x => x.FirstName, m => m.Length(200));
            Property(x => x.LastName, m =>
            {
                m.Length(200);
                m.NotNullable(true);
            });
            Bag(x => x.Albums, c =>
            {
                // Make sure to set Inverse(true) to prevent NHibernate from trying to break the association between
                // a Singer and an Album by setting Album.SingerId = NULL.
                c.Inverse(true);
                c.Key(k => k.Column("SingerId"));
            }, r => r.OneToMany());
        }
    }
}
