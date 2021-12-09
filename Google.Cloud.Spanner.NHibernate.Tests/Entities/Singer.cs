using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;
using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate.Tests.Entities
{
    public class Singer
    {
        public virtual long SingerId { get; set; }
        
        public virtual string FirstName { get; set; }
        
        public virtual string LastName { get; set; }
        
        public virtual string FullName { get; set; }
        
        public virtual SpannerDate BirthDate { get; set; }
        
        public virtual byte[] Picture { get; set; }

        public virtual IList<Album> Albums { get; set; }
    }

    public class SingerMapping : ClassMapping<Singer>
    {
        public SingerMapping()
        {
            Id(x => x.SingerId);
            Property(x => x.FirstName);
            Property(x => x.LastName);
            Property(x => x.FullName, mapper =>
            {
                mapper.Generated(PropertyGeneration.Always);
                mapper.Index("Idx_Singers_FullName");
            });
            Property(x => x.BirthDate);
            Property(x => x.Picture);
            Bag(x => x.Albums,
                c => c.Key(k => k.Column("SingerId")),
            r => r.OneToMany());
        }
    }
}
