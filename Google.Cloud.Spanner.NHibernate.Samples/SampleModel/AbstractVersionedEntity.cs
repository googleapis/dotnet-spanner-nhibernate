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

namespace Google.Cloud.Spanner.NHibernate.Samples.SampleModel
{
    /// <summary>
    /// Abstract base class for entities that uses a client side version number as concurrency token.
    /// </summary>
    public abstract class AbstractVersionedEntity
    {
        /// <summary>
        /// The primary key is a client-side generated UUID. Cloud Spanner does not support any server side key
        /// generation. The key value must therefore be set or generated client side. Monotonically increasing
        /// values are NOT recommended for PRIMARY KEYs.
        /// See also: https://cloud.google.com/spanner/docs/schema-and-data-model#choosing_a_primary_key
        /// </summary>
        public virtual string Id { get; set; }
        
        /// <summary>
        /// Version number that is automatically increased by NHibernate when the entity is updated.
        /// This property is used as a concurrency token to let NHibernate detect when an entity has been
        /// updated after it was loaded into memory.
        /// </summary>
        public virtual int Version { get; set; }
        
        /// <summary>
        /// This timestamp is automatically filled by Cloud Spanner when a new row is inserted.
        /// </summary>
        public virtual DateTime CreatedAt { get; set; }
        /// <summary>
        /// This timestamp is automatically filled by Cloud Spanner when a row is updated.
        /// </summary>
        public virtual DateTime? LastUpdatedAt { get; set; }
    }

    public abstract class VersionedEntityMapping<T> : ClassMapping<T> where T : AbstractVersionedEntity
    {
        public VersionedEntityMapping()
        {
            Persister<SpannerSingleTableWithFixedValuesEntityPersister>();
            DynamicUpdate(true);
            Id(x => x.Id, m => m.Generator(new UUIDHexGeneratorDef()));
            Version(x => x.Version, m =>
            {
                m.Generated(VersionGeneration.Never);
                m.Insert(true);
            });
            Property(x => x.CreatedAt, m =>
            {
                // The following prevents that NHibernate assigns a value to this property when the entity is inserted.
                // The property will then be set to the default value by the persister when the entity is inserted.
                m.Insert(false);
                m.Column(c => c.Default("PENDING_COMMIT_TIMESTAMP()"));
            });
            Property(x => x.LastUpdatedAt, m =>
            {
                // The following prevents that NHibernate assigns a value to this property when the entity is updated.
                // The property will then be set to the default value by the persister when the entity is updated.
                m.Update(false); 
                m.Column(c => c.Default("PENDING_COMMIT_TIMESTAMP()"));
            });
        }
    }
}
