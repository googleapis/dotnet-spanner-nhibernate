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

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests.InterleavedTableTests.Entities
{
    [Serializable]
    public class TrackIdentifier
    {
        public TrackIdentifier()
        {
        }

        public TrackIdentifier(Album album)
        {
            Album = album;
            TrackId = Guid.NewGuid().ToString();
        }
        
        public virtual Album Album { get; private set; }
        public virtual string TrackId { get; private set; }

        public override bool Equals(object other) =>
            other is TrackIdentifier ti 
            && Equals(Album?.AlbumIdentifier?.AlbumId, ti.Album?.AlbumIdentifier?.AlbumId)
            && Equals(TrackId, ti.TrackId);

        public override int GetHashCode() => 
            Album?.AlbumIdentifier?.AlbumId?.GetHashCode() ?? 0
            | TrackId?.GetHashCode() ?? 0;
    }
    public class Track
    {
        public Track()
        {
        }

        public virtual TrackIdentifier TrackIdentifier { get; set; }
        public virtual string Title { get; set; }
        public virtual Singer Singer => TrackIdentifier?.Album?.Singer;
        public virtual Album Album => TrackIdentifier?.Album;
    }

    public class TrackMapping : ClassMapping<Track>
    {
        public TrackMapping()
        {
            Table("Tracks");
            ComponentAsId(x => x.TrackIdentifier, m =>
            {
                m.ManyToOne(a => a.Album, mapping => mapping.Columns(
                    c1 => c1.Name("SingerId"),
                    c2 => c2.Name("AlbumId")
                ));
                m.Property(a => a.TrackId);
            });
            Property(x => x.Title);
        }
    }
}
