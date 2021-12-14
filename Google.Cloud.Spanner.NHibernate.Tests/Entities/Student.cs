using NHibernate.Mapping.ByCode.Conformist;

namespace Google.Cloud.Spanner.NHibernate.Tests.Entities
{
    public class Student : Person
    {
        public virtual long StudentNumber { get; set; }
    }

    public class StudentMapping : SubclassMapping<Student>
    {
        public StudentMapping()
        {
            DiscriminatorValue("Student");
            DynamicUpdate(true);
            Property(x => x.StudentNumber);
        }
    }
}