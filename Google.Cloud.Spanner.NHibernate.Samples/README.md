# Samples for Google Cloud Spanner NHibernate
This project contains a number of samples for using NHibernate in combination with Google Cloud Spanner.

The Snippets directory contains standalone samples for commonly used features of NHibernate and Cloud Spanner.
Browse these samples to get an impression of how the integration works or use it as a reference for best practices when implementing your own application.

The SampleModel directory contains the data model and mapping that is used with the samples.
All mapping is created in code using `NHibernate.Mapping.ByCode`.

## Running a Sample
The samples can be executed using the command `dotnet run <SampleName>` from this directory. The sample runner will automatically download and
start an instance of the Spanner emulator in a docker container, create the sample database and run the sample.

### Example
```
$ dotnet run Quickstart
Running sample Quickstart

Starting emulator...

Added singer Jamie Yngvason with id 2bd1ab0524e447f7ab986d926b94f8ba.

Stopping emulator...
```

### Prerequisites
The sample runner requires that Docker is installed on the local system, as it starts a Cloud Spanner emulator in a Docker container.
