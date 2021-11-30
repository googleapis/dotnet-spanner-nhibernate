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

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// This enum is used to determine when the Cloud Spanner NHibernate driver should
    /// use Mutations and when it should use DML for inserts/updates/deletes. DML statements allow
    /// transactions to read their own writes, but DML statements are slow in comparison
    /// mutations, especially for large batches of small updates. Mutations do not allow
    /// read-your-writes semantics, as mutations are buffered in the client until Commit is called,
    /// but mutations execute significantly faster on the backend.
    /// 
    /// The Cloud Spanner NHibernate driver therefore defaults to using mutations for
    /// implicit transactions, that is: when the application does not manually start a transaction
    /// on the session. With implicit transactions the NHibernate provider automatically starts
    /// a transaction when the session is flushed and commits this transaction if all operations succeeded.
    ///
    /// When the application manually starts a transaction, all inserts, updates and deletes will be
    /// executed as DML statements on the transaction. This allows the application to read the writes
    /// that have already been executed on the transaction.
    /// 
    /// An application can configure a session to use either DML or Mutations for all updates by
    /// calling ISession.UseMutations(MutationUsage).
    /// </summary>
    public enum MutationUsage
    {
        // Unspecified, use the default for the transaction or session.
        Unspecified,
        // Never use mutations, always use DML. This configuration is not recommended for most applications.
        Never,
        // Use mutations for implicit transactions and DML for manual transactions.
        // This is the default and is the appropriate configuration for most applications.
        ImplicitTransactions,
        // Always use mutations, never use DML. This will disable read-your-writes for manual transactions.
        // Use this for sessions that execute a large number of updates in manual transactions, if these
        // transactions do not need to read their own writes.
        Always
    }
}