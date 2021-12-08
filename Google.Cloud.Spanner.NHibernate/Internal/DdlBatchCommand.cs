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

using Google.Api.Gax;
using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate
{
    internal sealed class DdlBatchCommand : SpannerRetriableCommand
    {
        private readonly LinkedList<string> _statements = new LinkedList<string>();

        public DdlBatchCommand(SpannerRetriableConnection connection, SpannerCommand spannerCommand) : base(spannerCommand)
        {
            DbConnection = GaxPreconditions.CheckNotNull(connection, nameof(connection));
        }

        public override int ExecuteNonQuery()
        {
            _statements.AddLast(CommandText);
            return 0;
        }

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            _statements.AddLast(CommandText);
            return Task.FromResult(0);
        }

        protected override void Dispose(bool disposing)
        {
            if (_statements.Count > 0)
            {
                var spannerConnection = (SpannerRetriableConnection)DbConnection;
                var cmd = spannerConnection.CreateDdlCommand(_statements.First!.Value, _statements.Skip(1).ToArray());
                cmd.ExecuteNonQuery();
            }
            base.Dispose(disposing);
        }
    }
}