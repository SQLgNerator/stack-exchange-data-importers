param (
    [string]$DatabaseName,
    [string]$ServerName,
    [int]$BatchSize = 5000,
    [string]$DataDumpLoc
)

<#
    The function for pushing the data in the datatable to the server
#>
function pushDataToServer
{
  param(
  [string]$TableName,
  [string]$ConnectionString,
  [System.Data.DataTable]$Data
  )

  try{
        # create a object for establishing connection with the database
        $connection = New-Object System.Data.SqlClient.SQLConnection $ConnectionString
        
        # push the data in the datatable to the server 
        $connection.Open()
        $sqlBulkCopy = New-Object ("Data.SqlClient.SqlBulkCopy") $ConnectionString
        $sqlBulkCopy.DestinationTableName = $TableName
        $sqlBulkCopy.BatchSize = $BatchSize
        $sqlBulkCopy.WriteToServer($Data)
        $connection.Close()
  }
  catch{
        $ex = $_.Exception
        Write-Error "$ex.Message"  
        Exit
    }
}

<#
    Function for initialising the data column values with null values,later these
    values will be replaced by actual values in the xml
#>
function Initialise-DataTable($dataTable)
{
    # iterates through each colum and set the default value to null
    foreach($dataCol in $dataTable.Columns)
    {
        $dataCol.DefaultValue = [DBNull]::Value
    }
}

<#
    Function for filling the data row with the values in the xml
#>
function Fill-DataRows([System.Data.DataRow]$row,[System.Xml.XmlElement]$xmldata)
{
    # get the attributes from the xml node
    $rowAttributes = $xmldata.attributes

    # iterates through each nodes and get the values corresponds that attribute
    foreach($attr in $rowAttributes)
    {
       $row[$attr.Name] = $attr.Value
    }

    # this value will be returned from the function
   # $rowcl
}

<#
    The function will load the data from the xml to the specified datatable and then pushed to
    the specified table name in the database
#>
function Load-Xml-MSSQLServer($dataRows,$dataTable,$tableName)
{
    Write-Host "Rows to be inserted :- " + $dataRows.count

    $connectionString = "Data Source="+$ServerName+"; Database="+$DatabaseName+";Trusted_Connection=True;Connect Timeout=3000"

    $actualRowsInserted = 0 #specifies the actual number of rows inserted into the database 
    $totalRowsToBeInserted = $dataRows.count # specifies the total number of rows to be inserted into the database 

    # loop untill all the required rows are insterted into the database
    while($actualRowsInserted -lt $totalRowsToBeInserted)
    {
        Write-Host 'Processing row '$actualRowsInserted

        # create a new row for the data table
        $newRow = $dataTable.NewRow();
        Fill-DataRows $newRow $dataRows[$actualRowsInserted]

        # add the new row to the datatable 
        $dataTable.Rows.Add($newRow)

        # increase the counter for actual rows inserted by 1
        $actualRowsInserted +=1

        # check whether the total rows instered has reached the maximum threshold value
        if(($actualRowsInserted -ge $totalRowsToBeInserted-1) -or ($actualRowsInserted % $BatchSize -eq 0))
        {
            pushDataToServer $tableName $connectionString $dataTable
        
            # clear all rows in the data table
            $dataTable.Rows.Clear()
        
            Write-Host 'data pushed to server'
       }
    }
}

# verify the name of the database to which the data is to be exported 
if(-not $DatabaseName){
    Write-Host 'The database name cannot be empty.'
    Exit
}

# verify the name of the database server to be connected
if(-not $ServerName){
    Write-Host 'The server name cannot be empty.'
    Exit
}

# verify the location to the data dump(*.xml files)
if(-not ($DataDumpLoc -and (Test-Path $DataDumpLoc))){
    Write-Host 'Could not find the specified location for the data dump'
    Exit
}

#create a datatable for the 'Post' table in the database
$postTable = New-Object System.Data.DataTable

#define columns for the datatable
$postTable.Columns.Add("Id",[int])
$postTable.Columns.Add("PostTypeId",[int])
$postTable.Columns.Add("AcceptedAnswerId",[int])
$postTable.Columns.Add("ParentId",[int])
$postTable.Columns.Add("CreationDate",[DateTime])
$postTable.Columns.Add("Score",[int])
$postTable.Columns.Add("ViewCount",[int])
$postTable.Columns.Add("Body")
$postTable.Columns.Add("OwnerUserId",[int])
$postTable.Columns.Add("OwnerDisplayName")
$postTable.Columns.Add("LastEditorUserId",[int])
$postTable.Columns.Add("LastEditorDisplayName")
$postTable.Columns.Add("LastEditDate",[DateTime])
$postTable.Columns.Add("LastActivityDate",[DateTime])
$postTable.Columns.Add("Title")
$postTable.Columns.Add("Tags")
$postTable.Columns.Add("AnswerCount",[int])
$postTable.Columns.Add("CommentCount",[int])
$postTable.Columns.Add("FavoriteCount",[int])
$postTable.Columns.Add("ClosedDate",[DateTime])
$postTable.Columns.Add("CommunityOwnedDate",[DateTime])

# the data table to hold the data to be inserted into post history table
$postHistoryTable = New-Object System.Data.DataTable

# define the columns in the post history datatable
$postHistoryTable.Columns.Add("Id",[int])
$postHistoryTable.Columns.Add("Comment")
$postHistoryTable.Columns.Add("CreationDate",[DateTime])
$postHistoryTable.Columns.Add("PostHistoryTypeId",[int])
$postHistoryTable.Columns.Add("PostId",[int])
$postHistoryTable.Columns.Add("RevisionGUID",[Guid])
$postHistoryTable.Columns.Add("Text")
$postHistoryTable.Columns.Add("UserDisplayName")
$postHistoryTable.Columns.Add("UserId",[int])

# the data table to hold the data to be inserted into post history table
$commentsTable = New-Object System.Data.DataTable

#define the columns for Comments table
$commentsTable.Columns.Add("Id",[int])
$commentsTable.Columns.Add("CreationDate",[DateTime])
$commentsTable.Columns.Add("PostId",[int])
$commentsTable.Columns.Add("Score",[int])
$commentsTable.Columns.Add("Text")
$commentsTable.Columns.Add("UserDisplayName")
$commentsTable.Columns.Add("UserId",[int])

# the data table to hold the data to be inserted into badges table
$badgesTable = New-Object System.Data.DataTable

# define the columns for Badges table
$badgesTable.Columns.Add("Id",[int])
$badgesTable.Columns.Add("Date",[DateTime])
$badgesTable.Columns.Add("Name")
$badgesTable.Columns.Add("UserId",[int])

# the data table to hold the data to be inserted into post links table
$postLinksTable = New-Object System.Data.DataTable

# define the columns for PostLinks table
$postLinksTable.Columns.Add("Id",[int])
$postLinksTable.Columns.Add("CreationDate",[DateTime])
$postLinksTable.Columns.Add("LinkTypeId",[int])
$postLinksTable.Columns.Add("PostId",[int])
$postLinksTable.Columns.Add("RelatedPostId",[int])

# the data table to hold the data to be inserted into post links table
$tagsTable = New-Object System.Data.DataTable

# define the columns for Tags table
$tagsTable.Columns.Add("Id",[int])
$tagsTable.Columns.Add("Count",[int])
$tagsTable.Columns.Add("ExcerptPostId",[int])
$tagsTable.Columns.Add("TagName")
$tagsTable.Columns.Add("WikiPostId",[int])

# the tata table to hold the data to be inserted in to users table
$usersTable = New-Object System.Data.DataTable

#define the columns for user table
$usersTable.Columns.Add("Id",[int])
$usersTable.Columns.Add("AboutMe")
$usersTable.Columns.Add("AccountId",[int])
$usersTable.Columns.Add("Age",[int])
$usersTable.Columns.Add("CreationDate",[DateTime])
$usersTable.Columns.Add("DisplayName")
$usersTable.Columns.Add("DownVotes",[int])
$usersTable.Columns.Add("EmailHash")
$usersTable.Columns.Add("LastAccessDate",[DateTime])
$usersTable.Columns.Add("Location")
$usersTable.Columns.Add("ProfileImageUrl")
$usersTable.Columns.Add("Reputation",[int])
$usersTable.Columns.Add("UpVotes",[int])
$usersTable.Columns.Add("Views",[int])
$usersTable.Columns.Add("WebsiteUrl")

# the data table to hold the data for votes table 
$votesTable = New-Object System.Data.DataTable

#define the columns for votes table
$votesTable.Columns.Add("Id",[int])
$votesTable.Columns.Add("BountyAmount",[int])
$votesTable.Columns.Add("CreationDate",[DateTime])
$votesTable.Columns.Add("PostId",[int])
$votesTable.Columns.Add("UserId",[int])
$votesTable.Columns.Add("VoteTypeId",[int])

# initialise all the data tables with the default value null
Initialise-DataTable($postTable)
Initialise-DataTable($postHistoryTable)
Initialise-DataTable($commentsTable)
Initialise-DataTable($badgesTable)
Initialise-DataTable($postLinksTable)
Initialise-DataTable($tagsTable)
Initialise-DataTable($usersTable)
Initialise-DataTable($votesTable)

$postFile = $DataDumpLoc+'/Posts.xml'

# check for the post.xml file in the dump location
if(-not (Test-Path $postFile)){
    Write-Host "Could not find a "+ $postFile+" in the specified dump location."
    Exit
}

# Now read the posts.xml from the specified location and fill in the datatable
[xml]$postXmlData = Get-Content $postFile
$dataRows = $postXmlData.posts.row

Load-Xml-MSSQLServer $dataRows $postTable "Posts"

# delete the variables used for loading post data form the scope so that they are Garbage collected, and hence getting some free memory
Remove-Variable postXmlData
Remove-Variable dataRows

# get the file path to the "PostHistory.xml" file
$postHistoryFile = $DataDumpLoc+"/PostHistory.xml"

# check for the post.xml file in the dump location
if(-not (Test-Path $postHistoryFile)){
    Write-Host "Could not find a "+ $postHistoryFile+" in the specified dump location."
    Exit
}

# load the xml for 'PostHistory' details
[xml]$postHistoryXmlData = Get-Content $postHistoryFile
$postHistoryRows = $postHistoryXmlData.posthistory.row

Load-Xml-MSSQLServer $postHistoryRows $postHistoryTable "PostHistory"

# delete the variables used for loading post history data form the scope so that they are Garbage collected, and hence getting some free memory
Remove-Variable postHistoryXmlData
Remove-Variable postHistoryRows

# get the file path to the "Comments.xml" file
$commentsFile = $DataDumpLoc+"/Comments.xml"

# check for the post.xml file in the dump location
if(-not (Test-Path $commentsFile)){
    Write-Host "Could not find a "+ $commentsFile+" in the specified dump location."
    Exit
}

# load the xml file for comments
[xml]$commentsXmlData = Get-Content $commentsFile
$commentsRows = $commentsXmlData.comments.row

Load-Xml-MSSQLServer $commentsRows $commentsTable "Comments"

# release the variable used for loading the comments xml so that they are garbage collected
Remove-Variable commentsXmlData
Remove-Variable commentsRows

# get the file path to the "Comments.xml" file
$badgesFile = $DataDumpLoc+"/Badges.xml"

# check for the post.xml file in the dump location
if(-not (Test-Path $badgesFile)){
    Write-Host "Could not find a "+ $badgesFile+" in the specified dump location."
    Exit
}

# load the badges xml details from the location
[xml]$badgesXmlData = Get-Content $badgesFile
$badgesRows = $badgesXmlData.badges.row

Load-Xml-MSSQLServer $badgesRows $badgesTable "Badges"

# release the variable used for loading the badges xml so that they are garbage collected
Remove-Variable badgesXmlData
Remove-Variable badgesRows

# get the file path for "PostLinks.xml" file
$postLinksFile = $DataDumpLoc+"/PostLinks.xml"

# check for the PostLinks.xml file in the dump location
if(-not (Test-Path $postLinksFile)){
    Write-Host "Could not find a "+ $postLinksFile+" in the specified dump location."
    Exit
}

# load the post links details from the xml file
[xml]$postLinksXmlData = Get-Content $postLinksFile
$postLinksRows = $postLinksXmlData.postlinks.row

Load-Xml-MSSQLServer $postLinksRows $postLinksTable "PostLinks"

# release the variable used for loading the post links xml so that they are garbage collected
Remove-Variable postLinksXmlData
Remove-Variable postLinksRows

# get the file path for "Tags.xml" file
$tagsFile = $DataDumpLoc+"/Tags.xml"

# check for the Tags.xml file in the dump location
if(-not (Test-Path $tagsFile)){
    Write-Host "Could not find a "+ $tagsFile+" in the specified dump location."
    Exit
}

# laod the tag details from the xml file
[xml]$tagsXmlData = Get-Content $tagsFile
$tagsRows = $tagsXmlData.tags.row

Load-Xml-MSSQLServer $tagsRows $tagsTable "Tags"

# release the variable used for loading the tags xml so that they are garbage collected
Remove-Variable tagsXmlData
Remove-Variable tagsRows

# get the file path for "Users.xml" file
$usersFile = $DataDumpLoc+"/Users.xml"

# check for the Users.xml file in the dump location
if(-not (Test-Path $usersFile)){
    Write-Host "Could not find a "+ $usersFile+" in the specified dump location."
    Exit
}

# load the user details from the xml 
[xml]$userXmlData = Get-Content $usersFile
$usersRows = $userXmlData.users.row

Load-Xml-MSSQLServer $usersRows $usersTable "Users"

# release the variable used for loading the user xml so that they are garbage collected
Remove-Variable userXmlData
Remove-Variable usersRows

# get the file path for "Users.xml" file
$votesFile = $DataDumpLoc+"/Votes.xml"

# check for the Votes.xml file in the dump location
if(-not (Test-Path $votesFile)){
    Write-Host "Could not find a "+ $votesFile+" in the specified dump location."
    Exit
}

# load the votes details from the xml
[xml]$votesXmlData = Get-Content $votesFile
$votesRows = $votesXmlData.votes.row

Load-Xml-MSSQLServer $votesRows $votesTable "Votes"

# release the variable used for loading the user xml so that they are garbage collected
Remove-Variable votesXmlData
Remove-Variable votesRows

Write-Host "Data has been pushed to the database successfully."



