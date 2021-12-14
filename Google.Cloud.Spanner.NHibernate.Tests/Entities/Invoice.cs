using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate.Tests.Entities
{
    public class Invoice : AbstractBaseEntity
    {
        public virtual string Customer { get; set; }
        
        public virtual IList<InvoiceLine> InvoiceLines { get; set; }
    }

    public class InvoiceMapping : AbstractBaseEntityMapping<Invoice>
    {
        public InvoiceMapping()
        {
            Table("Invoices");
            Property(x => x.Customer);
            List(x => x.InvoiceLines, m =>
            {
                // Make sure to set Inverse(true) to prevent NHibernate from trying to break the association between
                // an Invoice and an InvoiceLine by setting InvoiceLine.Id = NULL.
                m.Inverse(true);
                m.Key(k => k.Column("Id"));
            }, r => r.OneToMany());
        }
    }
}