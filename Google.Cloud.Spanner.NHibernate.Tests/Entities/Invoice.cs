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