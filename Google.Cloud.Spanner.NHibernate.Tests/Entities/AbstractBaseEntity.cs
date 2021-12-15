using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;
using System;

namespace Google.Cloud.Spanner.NHibernate.Tests.Entities
{
    public class AbstractBaseEntity
    {
        public virtual string Id { get; set; }
        public virtual int Version { get; set; }
        public virtual DateTime CreatedAt { get; set; }
        public virtual DateTime? LastUpdatedAt { get; set; }
    }
    
    public class AbstractBaseEntityMapping<T> : ClassMapping<T> where T : AbstractBaseEntity
    {
        public AbstractBaseEntityMapping() : this(true)
        {
        }

        public AbstractBaseEntityMapping(bool includeIdMapping)
        {
            Persister<SpannerSingleTableEntityPersister>();
            DynamicUpdate(true);
            if (includeIdMapping)
            {
                Id(x => x.Id, m => m.Generator(new UUIDHexGeneratorDef()));
            }
            Version(x => x.Version, m =>
            {
                m.Generated(VersionGeneration.Never);
                m.Insert(true);
            });
            Property(x => x.CreatedAt, m =>
            {
                m.Insert(false);
                m.Column(c => c.Default("PENDING_COMMIT_TIMESTAMP()"));
            });
            Property(x => x.LastUpdatedAt, m =>
            {
                m.Update(false); 
                m.Column(c => c.Default("PENDING_COMMIT_TIMESTAMP()"));
            });
        }
    }
}