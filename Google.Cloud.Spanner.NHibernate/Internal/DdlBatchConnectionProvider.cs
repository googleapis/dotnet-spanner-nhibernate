// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.Spanner.Connection;
using NHibernate.Cfg;
using NHibernate.Connection;
using NHibernate.Driver;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Internal
{
    /// <summary>
    /// !!! ONLY INTENDED FOR INTERNAL USAGE !!!
    /// 
    /// This class is declared public for technical reasons to allow NHibernate to instantiate it through reflection.
    /// It should not be used in applications.
    /// 
    /// Connection provider that will group all DDL statements together and execute these as one batch when the command
    /// is disposed.  
    /// </summary>
    public sealed class DdlBatchConnectionProvider : IConnectionProvider
    {
        private IConnectionProvider _provider;

        public void Dispose() => _provider?.Dispose();

        public Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken) =>
            Task.FromResult(GetConnection());

        public void Configure(IDictionary<string, string> settings)
        {
            var newSettings = new Dictionary<string, string>(settings);
            if (settings.TryGetValue($"wrapped.{Environment.ConnectionProvider}", out var providerClass))
            {
                newSettings[Environment.ConnectionProvider] = providerClass;
            }
            else
            {
                newSettings.Remove(Environment.ConnectionProvider);
            }
            _provider = ConnectionProviderFactory.NewConnectionProvider(newSettings);
            _provider.Configure(newSettings);
        }

        public void CloseConnection(DbConnection conn)
        {
            _provider?.CloseConnection(conn);
            if (conn is DdlBatchConnection { ExecutionException: { } } ddlBatchConnection)
            {
                throw ddlBatchConnection.ExecutionException;
            }
        } 

        public DbConnection GetConnection()
        {
            var connection = _provider?.GetConnection();
            if (connection is DdlBatchConnection ddlBatchConnection)
            {
                return ddlBatchConnection;
            }
            if (connection is SpannerRetriableConnection spannerRetriableConnection)
            {
                return new DdlBatchConnection(spannerRetriableConnection);
            }
            return connection;
        }

        public IDriver Driver => _provider?.Driver;
    }
}