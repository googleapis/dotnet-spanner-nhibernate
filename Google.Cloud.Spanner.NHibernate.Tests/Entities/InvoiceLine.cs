using System;

namespace Google.Cloud.Spanner.NHibernate.Tests.Entities
{
    [Serializable]
    public class InvoiceLineIdentifier
    {
        public InvoiceLineIdentifier()
        {
        }

        public InvoiceLineIdentifier(Invoice invoice, long lineNumber)
        {
            Invoice = invoice;
            LineNumber = lineNumber;
        }
        
        public virtual Invoice Invoice { get; private set; }
        public virtual long LineNumber { get; private set; }

        public override bool Equals(object other) =>
            other is InvoiceLineIdentifier lineIdentifier && Equals(Invoice?.Id, lineIdentifier.Invoice?.Id) && Equals(LineNumber, lineIdentifier.LineNumber);

        // ReSharper disable twice NonReadonlyMemberInGetHashCode
        public override int GetHashCode() => Invoice?.Id?.GetHashCode() ?? 0 | LineNumber.GetHashCode();
    }
    
    public class InvoiceLine : AbstractBaseEntity
    {
        public virtual InvoiceLineIdentifier InvoiceLineIdentifier { get; set; }

        public override string Id => InvoiceLineIdentifier?.Invoice?.Id;

        public virtual Invoice Invoice => InvoiceLineIdentifier?.Invoice;

        public virtual long LineNumber => InvoiceLineIdentifier?.LineNumber ?? 0L;

        public virtual string Product { get; set; }
    }

    public class InvoiceLineMapping : AbstractBaseEntityMapping<InvoiceLine>
    {
        public InvoiceLineMapping() : base(false) // Skip the standard Id mapping
        {
            Table("InvoiceLines");
            ComponentAsId(x => x.InvoiceLineIdentifier, m =>
            {
                m.ManyToOne(id => id.Invoice, mapping => mapping.Column("Id"));
                m.Property(id => id.LineNumber);
            });
            Property(x => x.Product);
        }
    }
}