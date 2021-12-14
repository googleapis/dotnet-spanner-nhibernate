using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Impl;

namespace Google.Cloud.Spanner.NHibernate.Tests.Entities
{
    public abstract class Person : AbstractBaseEntity
    {
        public virtual string FirstName { get; set; }
        public virtual string LastName { get; set; }
        public virtual string FullName { get; set; }
    }
    
    public class PersonMapping : AbstractBaseEntityMapping<Person>
    {
        public PersonMapping()
        {
            Table("Persons");
            Discriminator(m => m.Column("PersonType"));
            Property(x => x.FirstName);
            Property(x => x.LastName);
            Property(x => x.FullName, mapper => mapper.Generated(PropertyGeneration.Always));
        }
    }
}