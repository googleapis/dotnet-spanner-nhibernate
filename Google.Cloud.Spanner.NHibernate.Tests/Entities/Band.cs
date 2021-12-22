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

using NHibernate.Mapping.ByCode.Conformist;
using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate.Tests.Entities
{
    public class Band
    {
        public Band()
        {
        }

        public virtual long BandId { get; set; }
        public virtual string Name { get; set; }
        public virtual IList<Singer> Members { get; set; } 
    }

    public class BandMapping : ClassMapping<Band>
    {
        public BandMapping()
        {
            Id(x => x.BandId, m => m.Column(c => c.NotNullable(true)));
            Property(x => x.Name);
            Bag(x => x.Members, c =>
            {
            }, r =>
            {
                r.ManyToMany(manyToMany =>
                {
                });
            });
        }
    }
}
