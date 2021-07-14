DROP TABLE IF EXISTS dbo.Programs_list
DROP TABLE IF EXISTS dbo.Toolings_list

CREATE TABLE dbo.Programs_list
(
  ONumber [NVARCHAR](255) NOT NULL,
  ModelNum [NVARCHAR](255),
  PartsName [NVARCHAR](255),
  GoodsName [NVARCHAR](255),
  FilesName [NVARCHAR](255),
  ItemCode [NVARCHAR](255),
  Tools INT,
  Creator [NVARCHAR](255),
  Tooling [NVARCHAR](255),
  FolderPath [NVARCHAR](255),
  CreateDate [NVARCHAR](50),
  ProcessTime [NVARCHAR](50)
);

CREATE TABLE dbo.Toolings_list
(
  ONumber [NVARCHAR](255) NOT NULL,
  ItemCode [NVARCHAR](255),
  FilesName [NVARCHAR](255),
  CreateDate [NVARCHAR](50),
  Tooling [NVARCHAR](255),
  FolderPath [NVARCHAR](255),
  TNumber [NVARCHAR](255),
  ToolName [NVARCHAR](255),
  HolderName [NVARCHAR](255),
  CutDistance FLOAT
);
