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
    [Serializable]
    public class TrackIdentifier
    {
        public TrackIdentifier()
        {
        }

        public TrackIdentifier(Album album, long trackNumber)
        {
            Album = album;
            TrackNumber = trackNumber;
        }
        
        public virtual Album Album { get; private set; }
        public virtual long TrackNumber { get; private set; }

        public override bool Equals(object other) =>
            other is TrackIdentifier trackIdentifier && Equals(Album?.Id, trackIdentifier.Album?.Id) && Equals(TrackNumber, trackIdentifier.TrackNumber);

        // ReSharper disable twice NonReadonlyMemberInGetHashCode
        public override int GetHashCode() => Album?.Id?.GetHashCode() ?? 0 | TrackNumber.GetHashCode();
    }
    
    public class Track : AbstractVersionedEntity
    {
        public virtual TrackIdentifier TrackIdentifier { get; set; }

        public override string Id => TrackIdentifier?.Album?.Id;
        
        public virtual Album Album => TrackIdentifier?.Album;

        public virtual long TrackNumber => TrackIdentifier?.TrackNumber ?? 0L;

        public virtual string Title { get; set; }

        /// <summary>
        /// NUMERIC columns are mapped to <see cref="V1.SpannerNumeric"/> by default.
        /// </summary>
        public virtual SpannerNumeric Duration { get; set; }

        /// <summary>
        /// ARRAY columns are mapped using custom types. Note that both the column itself (i.e. the underlying List)
        /// can be null, as well as each individual element in the array itself. Defining an ARRAY column as NOT NULL
        /// will make the column itself not nullable, but each array element could still be null.
        /// </summary>
        public virtual SpannerStringArray LyricsLanguages { get; set; }

        public virtual SpannerStringArray Lyrics { get; set; }

        public virtual IList<Performance> Performances { get; set; }
    }

    public class TrackMapping : VersionedEntityMapping<Track>
    {
        public TrackMapping() : base(false)
        {
            Table("Tracks");
            // The primary key of the table Tracks consists of the columns (Id, TrackNumber). The Id column also
            // references a row in the Albums table (the parent table).
            ComponentAsId(x => x.TrackIdentifier, m =>
            {
                m.ManyToOne(id => id.Album, mapping => mapping.Column("Id"));
                m.Property(id => id.TrackNumber);
            });
            Property(x => x.Title);
            Property(x => x.Duration);
            Property(x => x.LyricsLanguages);
            Property(x => x.Lyrics);
            Bag(x => x.Performances, c =>
            {
                c.Inverse(true);
                c.Key(k =>
                {
                    // The relationship Performance => Track is defined by the (AlbumId, TrackNumber) columns.
                    k.Columns(
                        column => column.Name("AlbumId"),
                        column => column.Name("TrackNumber"));
                });
            }, r => r.OneToMany());
        }
    }

}
