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
using System;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests.SampleEntities
{
    public class Performance
    {
        public virtual string Id { get; set; }
        public virtual Concert Concert { get; set; }
        public virtual Track Track { get; set; }
        public virtual DateTime? StartTime { get; set; }
        public virtual double? Rating { get; set; }
    }

    public class PerformanceMapping : ClassMapping<Performance>
    {
        public PerformanceMapping()
        {
            Persister<SpannerSingleTableEntityPersister>();
            DynamicUpdate(true);
            Table("Performances");
            Id(x => x.Id, m => m.Generator(new UUIDHexGeneratorDef()));
            ManyToOne(x => x.Concert);
            ManyToOne(x => x.Track);
            Property(x => x.StartTime);
            Property(x => x.Rating);
        }
    }
}
