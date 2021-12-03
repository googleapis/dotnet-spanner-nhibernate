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
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using NHibernate;
using NHibernate.AdoNet;
using NHibernate.Driver;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;
using System;
using System.Data;
using System.Data.Common;
using System.Threading;

namespace Google.Cloud.Spanner.NHibernate
{
    public class SpannerDriver : DriverBase, IEmbeddedBatcherFactoryProvider
    {
        private static readonly Lazy<SessionPoolManager> s_sessionPoolManager = new Lazy<SessionPoolManager>(() =>
        {
            var settings = SpannerSettings.GetDefault();
            settings.VersionHeaderBuilder.AppendAssemblyVersion("nhibernate", typeof(SpannerDriver));
            return SessionPoolManager.CreateWithSettings(new SessionPoolOptions(), settings);
        }, LazyThreadSafetyMode.ExecutionAndPublication);
        internal static SessionPoolManager SessionPoolManager { get; } = s_sessionPoolManager.Value;
        
        public SpannerDriver()
        {
        }

        public override DbConnection CreateConnection()
        {
            var connectionStringBuilder = new SpannerConnectionStringBuilder
            {
                SessionPoolManager = SessionPoolManager
            };
            var spannerConnection = new SpannerConnection(connectionStringBuilder);
            return new SpannerRetriableConnection(spannerConnection);
        }

        public override DbCommand CreateCommand() => new SpannerRetriableCommand(new SpannerCommand());

        public override bool UseNamedPrefixInSql => true;
        public override bool UseNamedPrefixInParameter => true;
        public override string NamedPrefix => "@";
        public System.Type BatcherFactoryClass => typeof(SpannerBatcherFactory);
        
        public override DbCommand GenerateCommand(CommandType type, SqlString sqlString, SqlType[] parameterTypes)
        {
            var sqlCommand = base.GenerateCommand(type, sqlString, parameterTypes);
            if (sqlString is SpannerMutationSqlString mutationSqlString)
            {
                return GenerateMutationOrDmlCommand((SpannerRetriableCommand)sqlCommand, mutationSqlString, parameterTypes);
            }
            return sqlCommand;
        }

        private DbCommand GenerateMutationOrDmlCommand(SpannerRetriableCommand dmlCommand, SpannerMutationSqlString sqlString, SqlType[] parameterTypes)
        {
            var cmd = new SpannerCommand();
            cmd.CommandText = sqlString.MutationCommandText;
            cmd.CommandType = dmlCommand.CommandType;
            SetCommandParameters(cmd, sqlString, parameterTypes);
            return new SpannerDmlOrMutationCommand(dmlCommand.SpannerCommand, new SpannerRetriableCommand(cmd));
        }

        private void SetCommandParameters(DbCommand cmd, SpannerMutationSqlString spannerMutationSqlString, SqlType[] sqlTypes)
        {
            for (int i = 0; i < sqlTypes.Length; i++)
            {
                var dbParam = GenerateParameter(cmd, spannerMutationSqlString.Columns[i], sqlTypes[i]);
                // Override the name that is generated.
                dbParam.ParameterName = spannerMutationSqlString.Columns[i];
                cmd.Parameters.Add(dbParam);
            }
            spannerMutationSqlString.AdditionalParameters.ForEach(p => cmd.Parameters.Add(p));
        }
        
        protected override void InitializeParameter(DbParameter dbParam, string name, SqlType sqlType)
        {
            if (sqlType == null)
            {
                throw new QueryException($"No type assigned to parameter '{name}'");
            }

            dbParam.ParameterName = FormatNameForParameter(name);
            if (dbParam is SpannerParameter spannerParameter)
            {
                if (sqlType is SpannerSqlType spannerSqlType)
                {
                    spannerParameter.SpannerDbType = spannerSqlType.SpannerDbType;
                }
                else
                {
                    spannerParameter.SpannerDbType = ToSpannerDbType(sqlType.DbType);
                }
            }
        }

        private static SpannerDbType ToSpannerDbType(DbType dbType) => dbType switch
        {
            DbType.Binary => SpannerDbType.Bytes,
            DbType.Boolean => SpannerDbType.Bool,
            DbType.Date => SpannerDbType.Date,
            DbType.DateTime => SpannerDbType.Timestamp,
            DbType.Single => SpannerDbType.Float64,
            DbType.Double => SpannerDbType.Float64,
            DbType.Int16 => SpannerDbType.Int64,
            DbType.Int32 => SpannerDbType.Int64,
            DbType.Int64 => SpannerDbType.Int64,
            DbType.VarNumeric => SpannerDbType.Numeric,
            DbType.Object => SpannerDbType.Unspecified,
            DbType.String => SpannerDbType.String,
            _ => SpannerDbType.Unspecified
        };
    }
}