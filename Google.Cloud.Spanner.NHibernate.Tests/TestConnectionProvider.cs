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
using Grpc.Core;
using System;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    internal class TestConnectionProvider : SpannerConnectionProvider
    {
        public override ChannelCredentials ChannelCredentials { get => ChannelCredentials.Insecure; set => throw new InvalidOperationException(); }

        // Always ignore the emulator settings, as we want to connect to a mock server.
        protected override EmulatorDetection EmulatorDetection => EmulatorDetection.None;
    }
}