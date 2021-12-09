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

using NHibernate.Mapping;
using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;
using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests.SampleEntities
{
    public class Album
    {
        public Album()
        {
        }

        public virtual string Id { get; set; }
        public virtual string Title { get; set; }
        public virtual SpannerDate ReleaseDate { get; set; }
        public virtual Singer Singer { get; set; }
        public virtual IList<Track> Tracks { get; set; }
    }

    public class AlbumMapping : ClassMapping<Album>
    {
        public AlbumMapping()
        {
            Persister<SpannerSingleTableEntityPersister>();
            DynamicUpdate(true);
            Table("Albums");
            Id(x => x.Id, m =>
            {
                m.Generator(new UUIDHexGeneratorDef());
                m.Length(36);
            });
            Property(x => x.Title, m =>
            {
                m.NotNullable(true);
                m.Length(100);
                m.Index("Idx_Albums_Title");
            });
            Property(x => x.ReleaseDate);
            ManyToOne(x => x.Singer, m =>
            {
                m.NotNullable(true);
                m.Column(c => c.Length(36));
                m.ForeignKey("FK_Albums_Singers");
            });
            Bag(x => x.Tracks, c => { }, r => r.OneToMany());
        }
    }
}
