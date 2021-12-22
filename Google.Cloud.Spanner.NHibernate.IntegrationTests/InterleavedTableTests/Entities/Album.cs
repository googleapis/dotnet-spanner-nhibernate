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

using NHibernate.Mapping.ByCode.Conformist;
using System;
using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests.InterleavedTableTests.Entities
{
    [Serializable]
    public class AlbumIdentifier
    {
        public AlbumIdentifier()
        {
        }

        public AlbumIdentifier(Singer singer)
        {
            Singer = singer;
            AlbumId = Guid.NewGuid().ToString();
        }
        
        public virtual Singer Singer { get; private set; }
        public virtual string AlbumId { get; private set; }

        public override bool Equals(object other) =>
            other is AlbumIdentifier ai && Equals(Singer?.SingerId, ai.Singer?.SingerId) && Equals(AlbumId, ai.AlbumId);

        public override int GetHashCode() => Singer?.SingerId?.GetHashCode() ?? 0 | AlbumId?.GetHashCode() ?? 0;
    }
    
    public class Album
    {
        public Album()
        {
        }

        public virtual AlbumIdentifier AlbumIdentifier { get; set; }
        public virtual string Title { get; set; }
        public virtual Singer Singer => AlbumIdentifier?.Singer;
        public virtual IList<Track> Tracks { get; set; }
    }

    public class AlbumMapping : ClassMapping<Album>
    {
        public AlbumMapping()
        {
            Persister<SpannerSingleTableEntityPersister>();
            Table("Albums");
            ComponentAsId(x => x.AlbumIdentifier, idMapper =>
            {
                idMapper.ManyToOne(a => a.Singer, manyToOneMapper =>
                {
                    manyToOneMapper.Column(c =>
                    {
                        c.Name("SingerId");
                        c.NotNullable(true);
                        c.Length(36);
                    });
                    manyToOneMapper.ForeignKey(InterleavedTableForeignKey.InterleaveInParent);
                });
                idMapper.Property(a => a.AlbumId, m =>
                {
                    m.Length(36);
                    m.NotNullable(true);
                });
            });
            Property(x => x.Title, m =>
            {
                m.Length(100);
                m.NotNullable(true);
            });
            Bag(x => x.Tracks,
                c =>
                {
                    // Make sure to set Inverse(true) to prevent NHibernate from trying to break the association between
                    // an Album and a Track by setting Track.AlbumId = NULL.
                    c.Inverse(true);
                    c.Key(k => k.Columns(c => c.Name("SingerId"), c => c.Name("AlbumId")));
                },
                r => r.OneToMany());
        }
    }
}
