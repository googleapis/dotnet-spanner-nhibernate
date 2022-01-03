// Copyright 2022 Google Inc. All Rights Reserved.
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

using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Environment = NHibernate.Cfg.Environment;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// NHibernate allows an application to configure the ADO.NET connection to use in three different ways:
    /// 1. Set a connection string and driver class in the configuration and let the driver supply the connection. This
    ///    is the recommended way to work with NHibernate in combination with Cloud Spanner.
    /// 2. Configure a custom connection provider in the NHibernate configuration and let that connection provider
    ///    create the connection.
    /// 3. Create a session with a user-supplied connection.
    ///
    /// The Cloud Spanner NHibernate driver and dialect only works with <see cref="SpannerRetriableConnection"/>. This
    /// connection class is a wrapper around the standard <see cref="SpannerConnection"/> that adds additional features
    /// that are required for NHibernate, such as additional schema metadata, internal retry logic for aborted
    /// transactions, read-only transactions etc.
    ///
    /// The Cloud Spanner NHibernate driver will automatically create the correct type of connection when no
    /// user-supplied connection or connection provider has been configured. It is important that your application
    /// creates an instance of <see cref="SpannerRetriableConnection"/> if you want to supply your own connections or
    /// connection providers.
    /// 
    /// Run from the command line with `dotnet run ConnectionProvider`
    /// </summary>
    public static class ConnectionProviderSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            // The default configuration will automatically use a DriverConnectionProvider, which again will request a
            // connection from the driver. The Cloud Spanner NHibernate driver will automatically create the required
            // SpannerRetriableConnection.
            using var session = configuration.SessionFactory.OpenSession();
            var driverSuppliedConnection = session.Connection;
            Console.WriteLine(
                $"The driver created a connection of type {driverSuppliedConnection.GetType().FullName}");

            // Your application can configure a custom connection provider that should be used to create connections.
            // Always make sure that this provider returns instances of SpannerRetriableConnection.
            configuration.Configuration.SetProperty(Environment.ConnectionProvider,
                typeof(SampleConnectionProvider).AssemblyQualifiedName);
            var sessionFactoryWithConnectionProvider = configuration.Configuration.BuildSessionFactory();
            using var sessionWithCustomConnectionProvider = sessionFactoryWithConnectionProvider.OpenSession();
            var providerSuppliedConnection = sessionWithCustomConnectionProvider.Connection;
            Console.WriteLine(
                $"The SampleConnectionProvider created a connection of type {providerSuppliedConnection.GetType().FullName}");

            // Your application can also create a session with a user-supplied connection. The connection must be a
            // SpannerRetriableConnection.
            var connectionString = configuration.Configuration.GetProperty(Environment.ConnectionString);
            var spannerConnection = new SpannerConnection(connectionString, ChannelCredentials.Insecure);
            // Wrap a SpannerConnection in a SpannerRetriableConnection to use with NHibernate.
            using var userSuppliedConnection = new SpannerRetriableConnection(spannerConnection);
            using var sessionWithUserSuppliedConnection = configuration.SessionFactory.WithOptions()
                .Connection(userSuppliedConnection).OpenSession();
            Console.WriteLine(
                $"The session with a user-supplied connection uses a connection of type {sessionWithUserSuppliedConnection.Connection.GetType().FullName}");
            
            await Task.CompletedTask;
        }
    }

    /// <summary>
    /// Example connection provider for the Cloud Spanner NHibernate driver. The connection provider must return an
    /// instance of a <see cref="SpannerRetriableConnection"/>. It is recommended to use the
    /// <see cref="SpannerConnectionProvider"/> as a base class if you want to supply your own connection provider.
    /// </summary>
    public class SampleConnectionProvider : SpannerConnectionProvider
    {
        public override void Configure(IDictionary<string, string> settings)
        {
            // Conditionally configure the connection provider to show the generated sql based on an environment
            // variable.
            if (Equals(System.Environment.GetEnvironmentVariable("SHOW_SQL"), "true"))
            {
                settings.Add(Environment.ShowSql, "true");
            }
            base.Configure(settings);
        }
    }
}