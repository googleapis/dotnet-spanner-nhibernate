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
using System;

namespace Google.Cloud.Spanner.NHibernate.Tests.Entities
{
    [Serializable]
    public class InvoiceLineNoteIdentifier
    {
        public InvoiceLineNoteIdentifier()
        {
        }

        public InvoiceLineNoteIdentifier(InvoiceLine invoiceLine, long noteNumber)
        {
            InvoiceLine = invoiceLine;
            NoteNumber = noteNumber;
        }
        
        public virtual InvoiceLine InvoiceLine { get; private set; }
        public virtual long NoteNumber { get; private set; }

        public override bool Equals(object other) =>
            other is InvoiceLineNoteIdentifier detailIdentifier
            && Equals(InvoiceLine?.Invoice?.Id, detailIdentifier.InvoiceLine?.Invoice?.Id)
            && Equals(InvoiceLine?.LineNumber, detailIdentifier.InvoiceLine?.LineNumber)
            && Equals(NoteNumber, detailIdentifier.NoteNumber);

        // ReSharper disable NonReadonlyMemberInGetHashCode
        public override int GetHashCode() => InvoiceLine?.Invoice?.Id?.GetHashCode() ?? 0
            | InvoiceLine?.LineNumber.GetHashCode() ?? 0 | NoteNumber.GetHashCode();
    }
    
    public class InvoiceLineNote : AbstractBaseEntity
    {
        public virtual InvoiceLineNoteIdentifier InvoiceLineNoteIdentifier { get; set; }

        public override string Id => InvoiceLineNoteIdentifier?.InvoiceLine?.Invoice?.Id;

        public virtual InvoiceLine InvoiceLine => InvoiceLineNoteIdentifier?.InvoiceLine;

        public virtual long NoteNumber => InvoiceLineNoteIdentifier?.NoteNumber ?? 0L;

        public virtual string Note { get; set; }
    }

    public class InvoiceLineNoteMapping : AbstractBaseEntityMapping<InvoiceLineNote>
    {
        public InvoiceLineNoteMapping() : base(false) // Skip the standard Id mapping
        {
            Table("InvoiceLineNotes");
            ComponentAsId(x => x.InvoiceLineNoteIdentifier, m =>
            {
                m.ManyToOne(id => id.InvoiceLine, mapping =>
                {
                    mapping.Columns(c =>
                    {
                        c.Name("Id");
                        c.NotNullable(true);
                        c.Length(36);
                    }, c =>
                    {
                        c.Name("LineNumber");
                        c.NotNullable(true);
                    });
                    mapping.ForeignKey(InterleavedTableForeignKey.InterleaveInParent);
                    mapping.Cascade(Cascade.DeleteOrphans);
                });
                m.Property(id => id.NoteNumber, mapping => mapping.NotNullable(true));
            });
            Property(x => x.Note, m => m.NotNullable(true));
        }
    }
}