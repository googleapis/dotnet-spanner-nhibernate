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

namespace Google.Cloud.Spanner.NHibernate.Samples.SampleModel
{
    /// <summary>
    /// BandMembership is a 'relationship entity' between Band and Singer to indicate which singers belong to which band.
    /// Prefer these kind of relationship entities over many-to-many relationships to have more control over the
    /// collection. An additional advantage of defining the relationship using a relationship entity is that you can
    /// easily add additional properties to the relationship, such as the begin- and end time of the relationship.
    /// </summary>
    public class BandMembership : AbstractVersionedEntity
    {
        public virtual Singer Singer { get; set; }
        
        public virtual Band Band { get; set; }

        public virtual DateTime BeginDate { get; set; }
        
        public virtual DateTime? EndDate { get; set; }
    }

    public class BandMembershipMapping : VersionedEntityMapping<BandMembership>
    {
        public BandMembershipMapping()
        {
            Table("BandMemberships");
            ManyToOne(x => x.Singer, m => m.Column("SingerId"));
            ManyToOne(x => x.Band, m => m.Column("BandId"));
            Property(x => x.BeginDate);
            Property(x => x.EndDate);
        }
    }
}
