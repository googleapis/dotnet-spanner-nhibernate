﻿// Copyright 2021 Google Inc. All Rights Reserved.
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
using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests.SampleEntities
{
    public class Venue
    {
        public Venue()
        {
        }

        public virtual string Code { get; set; }
        public virtual string Name { get; set; }
        public virtual bool Active { get; set; }
        public virtual long? Capacity { get; set; }
        public virtual SpannerFloat64Array Ratings { get; set; }

        public virtual IList<Concert> Concerts { get; set; }
    }

    public class VenueMapping : ClassMapping<Venue>
    {
        public VenueMapping()
        {
            Persister<SpannerSingleTableEntityPersister>();
            DynamicUpdate(true);
            Table("Venues");
            Id(x => x.Code, m => m.Length(10));
            Property(x => x.Name, m => m.Length(100));
            Property(x => x.Active, m => m.NotNullable(true));
            Property(x => x.Capacity);
            Property(x => x.Ratings);
            Bag(x => x.Concerts, c => { }, r => r.OneToMany());
        }
    }
}
