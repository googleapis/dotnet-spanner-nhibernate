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
            ManyToOne(x => x.Singer, m =>
            {
                m.Column(c =>
                {
                    c.Name("SingerId");
                    c.NotNullable(true);
                    c.Length(36);
                });
                m.ForeignKey("FK_Albums_Singers");
            });
            Property(x => x.Title, m =>
            {
                m.NotNullable(true);
                m.Length(100);
                m.Index("Idx_Albums_Title");
            });
            Property(x => x.ReleaseDate);
            Bag(x => x.Tracks,
                collectionMapping =>
                {
                    // Make sure to set Inverse(true) to prevent NHibernate from trying to break the association between
                    // an Album and a Track by setting the key values to NULL.
                    collectionMapping.Inverse(true);
                    // The Id column of a Track defines the Album that it belongs to.
                    collectionMapping.Key(key => key.Column("Id"));
                    // The tracks will be order by their track numbers.
                    collectionMapping.OrderBy("TrackNumber");
                },
                r => r.OneToMany());
        }
    }
}
