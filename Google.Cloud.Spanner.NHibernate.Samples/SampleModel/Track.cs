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

using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate.Samples.SampleModel
{
    public class Track : AbstractVersionedEntity
    {
        public virtual Album Album { get; set; }

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
        public TrackMapping()
        {
            Table("Tracks");
            ManyToOne(x => x.Album, m => m.Column("AlbumId"));
            Property(x => x.Title);
            Property(x => x.Duration);
            Property(x => x.LyricsLanguages);
            Property(x => x.Lyrics);
            Bag(x => x.Performances, c => { }, r => r.OneToMany());
        }
    }

}
