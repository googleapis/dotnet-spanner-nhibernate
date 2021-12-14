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

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests.SampleEntities
{
    public class Track
    {
        public Track()
        {
        }

        public virtual string Id { get; set; }
        public virtual Album Album { get; set; }
        public virtual string Title { get; set; }
        public virtual SpannerNumeric Duration { get; set; }
        public virtual SpannerStringArray LyricsLanguages { get; set; }
        public virtual SpannerStringArray Lyrics { get; set; }
        public virtual IList<Performance> Performances { get; set; }
    }

    public class TrackMapping : ClassMapping<Track>
    {
        public TrackMapping()
        {
            Persister<SpannerSingleTableEntityPersister>();
            DynamicUpdate(true);
            Table("Tracks");
            Id(x => x.Id, m =>
            {
                m.Generator(new UUIDHexGeneratorDef());
                m.Length(36);
            });
            ManyToOne(x => x.Album, m =>
            {
                m.NotNullable(true);
                m.Column(c => c.Length(36));
                m.ForeignKey("FK_Tracks_Albums");
                m.UniqueKey("Idx_Tracks_Album_Title");
            });
            Property(x => x.Title, m =>
            {
                m.NotNullable(true);
                m.Length(200);
                m.UniqueKey("Idx_Tracks_Album_Title");
            });
            Property(x => x.Duration);
            Property(x => x.LyricsLanguages, m => m.Length(2));
            Property(x => x.Lyrics);
            Bag(x => x.Performances, c => { }, r => r.OneToMany());
        }
    }
}
