/* Copyright 2021 Google LLC
* 
* Licensed under the Apache License, Version 2.0 (the "License")
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     https://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

CREATE TABLE Singers (
  Id        STRING(36) NOT NULL,
  FirstName STRING(200),
  LastName  STRING(200) NOT NULL,
  FullName  STRING(400) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED,
  BirthDate DATE,
  Picture   BYTES(MAX),
) PRIMARY KEY (Id);

CREATE INDEX Idx_Singers_FullName ON Singers (FullName);

CREATE TABLE Albums (
  Id          STRING(36) NOT NULL,
  Singer      STRING(36) NOT NULL,
  Title       STRING(100) NOT NULL,
  ReleaseDate DATE,
  CONSTRAINT  FK_Albums_Singers FOREIGN KEY (Singer) REFERENCES Singers (Id),
) PRIMARY KEY (Id);

CREATE UNIQUE INDEX Idx_Albums_Title ON Albums (Singer, Title);

CREATE TABLE Tracks (
  Id              STRING(36) NOT NULL,
  Album           STRING(36) NOT NULL,
  Title           STRING(200) NOT NULL,
  Duration        NUMERIC,
  LyricsLanguages ARRAY<STRING(2)>,
  Lyrics          ARRAY<STRING(MAX)>,
  CONSTRAINT      Chk_Languages_Lyrics_Length_Equal CHECK (ARRAY_LENGTH(LyricsLanguages) = ARRAY_LENGTH(Lyrics)),
  CONSTRAINT      FK_Tracks_Albums FOREIGN KEY (Album) REFERENCES Albums (Id),
) PRIMARY KEY (Id);

CREATE UNIQUE INDEX Idx_Tracks_Album_Title ON Tracks (Album, Title);

CREATE TABLE Venues (
  Code      STRING(10) NOT NULL,
  Name      STRING(100),
  Active    BOOL NOT NULL,
  Capacity  INT64,
  Ratings   ARRAY<FLOAT64>,
) PRIMARY KEY (Code);

CREATE TABLE Concerts (
  Id         STRING(36) NOT NULL,
  Venue      STRING(10) NOT NULL,
  StartTime  TIMESTAMP NOT NULL,
  Singer     STRING(36) NOT NULL,
  Title      STRING(200),
  CONSTRAINT FK_Concerts_Venues FOREIGN KEY (Venue) REFERENCES Venues (Code),
  CONSTRAINT FK_Concerts_Singers FOREIGN KEY (Singer) REFERENCES Singers (Id),
) PRIMARY KEY (Id);

CREATE TABLE Performances (
  Id               STRING(36) NOT NULL,
  Concert          STRING(36) NOT NULL,
  Track            STRING(36) NOT NULL,
  StartTime        TIMESTAMP,
  Rating           FLOAT64,
  CONSTRAINT FK_Performances_Concerts FOREIGN KEY (Concert) REFERENCES Concerts (Id),
  CONSTRAINT FK_Performances_Tracks FOREIGN KEY (Track) REFERENCES Tracks (Id),
) PRIMARY KEY (Id);

CREATE TABLE TableWithAllColumnTypes (
	ColInt64 INT64 NOT NULL,
	ColFloat64 FLOAT64,
	ColNumeric NUMERIC,
	ColBool BOOL,
	ColString STRING(100),
	ColStringMax STRING(MAX),
	ColBytes BYTES(100),
	ColBytesMax BYTES(MAX),
	ColDate DATE,
	ColTimestamp TIMESTAMP,
	ColCommitTs TIMESTAMP OPTIONS (allow_commit_timestamp=true),
	ColInt64Array ARRAY<INT64>,
	ColFloat64Array ARRAY<FLOAT64>,
	ColNumericArray ARRAY<NUMERIC>,
	ColBoolArray ARRAY<BOOL>,
	ColStringArray ARRAY<STRING(100)>,
	ColStringMaxArray ARRAY<STRING(MAX)>,
	ColBytesArray ARRAY<BYTES(100)>,
	ColBytesMaxArray ARRAY<BYTES(MAX)>,
	ColDateArray ARRAY<DATE>,
	ColTimestampArray ARRAY<TIMESTAMP>,
	ColComputed STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED,
	ColASC STRING(MAX),
) PRIMARY KEY (ColInt64);

CREATE NULL_FILTERED INDEX IDX_TableWithAllColumnTypes_ColDate_ColCommitTs ON TableWithAllColumnTypes (ColDate, ColCommitTs);

CREATE TABLE SingersWithVersion (
  Id        STRING(36) NOT NULL,    
  Version INT64 NOT NULL,
  FirstName STRING(200),
  LastName  STRING(200) NOT NULL,
  FullName  STRING(400) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED,
  BirthDate DATE,
  Picture   BYTES(MAX),
) PRIMARY KEY (Id);

CREATE INDEX Idx_SingersWithVersion_FullName ON SingersWithVersion (FullName);

CREATE TABLE AlbumsWithVersion (
  Id          STRING(36) NOT NULL, 
  Version INT64 NOT NULL,    
  Singer      STRING(36) NOT NULL,
  Title       STRING(100) NOT NULL,
  ReleaseDate DATE,
  CONSTRAINT  FK_Albums_Singers_WithVersion FOREIGN KEY (Singer) REFERENCES SingersWithVersion (Id),
) PRIMARY KEY (Id);

CREATE UNIQUE INDEX Idx_AlbumsWithVersions_Title ON AlbumsWithVersion (Singer, Title);