﻿// Copyright 2021 Google LLC
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
    public class Concert : AbstractVersionedEntity
    {
        public virtual Venue Venue { get; set; }
        /// <summary>
        /// TIMESTAMP columns are mapped by default to <see cref="DateTime"/>. This distinguishes
        /// them from DATE columns that by default are mapped to <see cref="SpannerDate"/>.
        /// </summary>
        public virtual DateTime StartTime { get; set; }
        public virtual Singer Singer { get; set; }
        public virtual string Title { get; set; }
        public virtual IList<Performance> Performances { get; set; }
    }

    public class ConcertMapping : VersionedEntityMapping<Concert>
    {
        public ConcertMapping()
        {
            Table("Concerts");
            ManyToOne(x => x.Venue, m =>
            {
                m.Column(c =>
                {
                    c.Name("VenueId");
                    c.NotNullable(true);
                    c.Length(36);
                });
                m.ForeignKey("FK_Concerts_Venues");
            });
            Property(x => x.StartTime, m => m.NotNullable(true));
            ManyToOne(x => x.Singer, m =>
            {
                m.Column(c =>
                {
                    c.Name("SingerId");
                    c.NotNullable(true);
                    c.Length(36);
                });
                m.ForeignKey("FK_Concerts_Singers");
            });
            Property(x => x.Title, m => m.Length(200));
            Bag(x => x.Performances, c =>
            {
                c.Inverse(true);
                c.Key(k => k.Column("ConcertId"));
            }, r => r.OneToMany());
        }
    }
}
