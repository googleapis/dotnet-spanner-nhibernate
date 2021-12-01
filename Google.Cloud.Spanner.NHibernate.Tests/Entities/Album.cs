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

namespace Google.Cloud.Spanner.NHibernate.Tests.Entities
{
    public class Album
    {
        public Album()
        {
        }

        public virtual long AlbumId { get; set; }
        public virtual string Title { get; set; }
        public virtual SpannerDate ReleaseDate { get; set; }
        public virtual Singer Singer { get; set; }
        public virtual IList<Track> Tracks { get; set; } 
    }

    public class AlbumMapping : ClassMapping<Album>
    {
        public AlbumMapping()
        {
            Id(x => x.AlbumId, m => m.Column(c => c.NotNullable(true)));
            Property(x => x.Title);
            Property(x => x.ReleaseDate);
            ManyToOne(x => x.Singer, m => m.Column("SingerId"));
            Bag(x => x.Tracks, c =>
            {
                // This prevents NHibernate from generating an `UPDATE Tracks SET AlbumId=null WHERE AlbumId=@p0` when
                // an Album is deleted.
                c.Key(k => k.NotNullable(true));
            }, r => r.OneToMany());
        }
    }
}
