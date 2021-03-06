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
                m.ManyToOne(id => id.Album, albumMapper =>
                {
                    albumMapper.Column(c =>
                    {
                        c.Name("Id");
                        c.NotNullable(true);
                        c.Length(36);
                    });
                    // Cloud Spanner does not support unique constraints. The Cloud Spanner Dialect will
                    // therefore automatically translate unique key constraint definitions to unique indexes.
                    // Add each column that should be included in a unique index to the unique key definition.
                    albumMapper.UniqueKey("Idx_Tracks_AlbumId_Title");
                    // Giving the foreign key exactly the name 'INTERLEAVE IN PARENT' tells the Cloud Spanner NHibernate
                    // dialect that it should see this relationship as an interleaved child table and not a normal
                    // foreign key constraint. This relationship also supports cascade deletes. This is enabled on the
                    // parent side of the relationship (in the Album mapping).
                    albumMapper.ForeignKey(InterleavedTableForeignKey.InterleaveInParent);
                });
                m.Property(id => id.TrackNumber, propertyMapper => propertyMapper.NotNullable(true));
            });
            Property(x => x.Title, m =>
            {
                m.NotNullable(true);
                m.Length(200);
                // This will also include the Title column in the Idx_Tracks_AlbumId_Title unique index.
                m.UniqueKey("Idx_Tracks_AlbumId_Title");
            });
            Property(x => x.Duration);
            Property(x => x.LyricsLanguages, m => m.Length(2));
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
