using NHibernate.Mapping.ByCode.Conformist;

namespace Google.Cloud.Spanner.NHibernate.Tests.Entities
{
    public class Teacher : Person
    {
        public virtual string Code { get; set; }
    }
    
    public class TeacherMapping : SubclassMapping<Teacher>
    {
        public TeacherMapping()
        {
            DiscriminatorValue("Teacher");
            DynamicUpdate(true);
            Property(x => x.Code);
        }
    }
}