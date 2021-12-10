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

using NHibernate.Mapping.ByCode;
using System;
using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate.Samples.SampleModel
{
    public class Album : AbstractVersionedEntity
    {
        public virtual Singer Singer { get; set; }
        
        public virtual string Title { get; set; }

        /// <summary>
        /// DATE columns are best mapped to <see cref="SpannerDate"/>.
        /// This makes it easier to distinguish them from TIMESTAMP columns, which are mapped to <see cref="DateTime"/>.
        /// </summary>
        public virtual SpannerDate ReleaseDate { get; set; }

        public virtual ICollection<Track> Tracks { get; set; }
    }

    public class AlbumMapping : VersionedEntityMapping<Album>
    {
        public AlbumMapping()
        {
            Table("Albums");
            ManyToOne(x => x.Singer, m => m.Column("SingerId"));
            Property(x => x.Title);
            Property(x => x.ReleaseDate);
            Bag(x => x.Tracks, c => { }, r => r.OneToMany());
        }
    }
}
