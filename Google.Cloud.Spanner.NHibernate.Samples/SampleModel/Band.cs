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
    public class Band : AbstractVersionedEntity
    {
        public virtual string Name { get; set; }

        public virtual ICollection<BandMembership> BandMemberships { get; set; }
    }

    public class BandMapping : VersionedEntityMapping<Band>
    {
        public BandMapping()
        {
            Table("Bands");
            Property(x => x.Name, m =>
            {
                m.NotNullable(true);
                m.Length(200);
            });
            Bag(x => x.BandMemberships,
                collectionMapping =>
                {
                    // Always set the one-to-many side of a collection mapping to Inverse(true).
                    // This prevents NHibernate from trying to delete and re-insert all elements in a collection when
                    // the collection is modified. This requires that the many-to-one side of the mapping always sets
                    // a value for the parent entity. That is, BandMember.Band must always be set when a new
                    // BandMembership is created.
                    collectionMapping.Inverse(true);
                    collectionMapping.Key(key => key.Column("BandId"));
                },
                r => r.OneToMany());
        }
    }
}
