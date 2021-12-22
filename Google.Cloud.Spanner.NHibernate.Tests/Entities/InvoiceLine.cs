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

using System;
using System.Collections.Generic;

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
        
        public virtual IList<InvoiceLineNote> InvoiceLineNotes { get; set; }
    }

    public class InvoiceLineMapping : AbstractBaseEntityMapping<InvoiceLine>
    {
        public InvoiceLineMapping() : base(false) // Skip the standard Id mapping
        {
            Table("InvoiceLines");
            ComponentAsId(x => x.InvoiceLineIdentifier, m =>
            {
                m.ManyToOne(id => id.Invoice, mapping =>
                {
                    mapping.Column("Id");
                    mapping.NotNullable(true);
                    mapping.ForeignKey(InterleavedTableForeignKey.InterleaveInParent);
                });
                m.Property(id => id.LineNumber, mapping => mapping.NotNullable(true));
            });
            Property(x => x.Product, m => m.NotNullable(true));
            Bag(x => x.InvoiceLineNotes, m =>
            {
                // Make sure to set Inverse(true) to prevent NHibernate from trying to break the association between
                // an Invoice and an InvoiceLine by setting InvoiceLine.Id = NULL.
                m.Inverse(true);
                m.Key(k =>
                {
                    k.Columns(c => c.Name("Id"), c => c.Name("LineNumber"));
                });
                m.OrderBy(x => x.NoteNumber);
            }, r => r.OneToMany());
        }
    }
}