/****** Object:  Table [dbo].[Posts]    Script Date: 09-02-2015 AM 01:08:38 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Posts](
	[Id] [int] NOT NULL,
	[PostTypeId] [tinyint] NULL,
	[AcceptedAnswerId] [int] NULL,
	[ParentId] [int] NULL,
	[CreationDate] [datetime] NULL,
	[Score] [int] NULL,
	[ViewCount] [int] NULL,
	[Body] [nvarchar](max) NULL,
	[OwnerUserId] [int] NULL,
	[OwnerDisplayName] [nvarchar](40) NULL,
	[LastEditorUserId] [int] NULL,
	[LastEditorDisplayName] [nvarchar](40) NULL,
	[LastEditDate] [datetime] NULL,
	[LastActivityDate] [datetime] NULL,
	[Title] [nvarchar](250) NULL,
	[Tags] [nvarchar](150) NULL,
	[AnswerCount] [int] NULL,
	[CommentCount] [int] NULL,
	[FavoriteCount] [int] NULL,
	[ClosedDate] [datetime] NULL,
	[CommunityOwnedDate] [datetime] NULL,
 CONSTRAINT [PK_Posts] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
