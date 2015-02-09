param (
    [string]$DatabaseName,
    [string]$ServerName,
    [int]$BatchSize = 5000,
    [string]$DataDumpLoc
)

function pushDataToServer
{
  param(
  [string]$TableName,
  [string]$ConnectionString,
  [System.Data.DataTable]$Data
  )

  try{
        $connection = New-Object System.Data.SqlClient.SQLConnection $ConnectionString
        
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
$postTable.Columns.Add("LastAcitvityDate",[DateTime])
$postTable.Columns.Add("Title")
$postTable.Columns.Add("Tags")
$postTable.Columns.Add("AnswerCount",[int])
$postTable.Columns.Add("CommentCount",[int])
$postTable.Columns.Add("FavoriteCount",[int])
$postTable.Columns.Add("ClosedDate",[DateTime])
$postTable.Columns.Add("CommunityOwnedDate",[DateTime])

$postFile = $DataDumpLoc+'/Posts.xml'

# check for the post.xml file in the dump location
if(-not (Test-Path $postFile)){
    Write-Host 'Could not find a '+ $postFile+' in the specified dump location.'
    Exit
}

# Now read the posts.xml from the specified location and fill in the datatable
[xml]$postXmlData = Get-Content $postFile
$dataRows = $postXmlData.posts.row

#$postXmlData.posts.row
$dataRows.count
$dataRows.length

$actualRowsInserted = 0 #specifies the actual number of rows inserted into the database 
$totalRowsToBeInserted = $dataRows.count # specifies the total number of rows to be inserted into the database 

$connectionString = "Data Source="+$ServerName+"; Database="+$DatabaseName+";Trusted_Connection=True;Connect Timeout=3000"

# loop untill all the required rows are insterted into the database
while($actualRowsInserted -lt $totalRowsToBeInserted)
{
    Write-Host 'Processing row '$dataRows[$actualRowsInserted].Id

    # create a new row for the data table
    $newPost = $postTable.NewRow();

    # fillin the mandatory fields corresponds to each questions/answers
    $newPost["Id"] = $dataRows[$actualRowsInserted].Id;
    $newPost["PostTypeId"] = $dataRows[$actualRowsInserted].PostTypeId
    $newPost["CreationDate"] = $dataRows[$actualRowsInserted].CreationDate
    $newPost["Score"] = $dataRows[$actualRowsInserted].Score
    $newPost["Body"] = $dataRows[$actualRowsInserted].Body
    $newPost["OwnerDisplayName"] = $dataRows[$actualRowsInserted].OwnerDisplayName
    $newPost["LastEditorDisplayName"] = $dataRows[$actualRowsInserted].LastEditorDisplayName
    $newPost["Title"] = $dataRows[$actualRowsInserted].Title
    $newPost["Tags"] = $dataRows[$actualRowsInserted].Tags
    $newPost["CommentCount"] = $dataRows[$actualRowsInserted].CommentCount

    # validate the ownerId field in the xml
    if($dataRows[$actualRowsInserted].OwnerUserId)
    {
        # the xml contains a valid owner user Id, add it to the datarow
        $newPost["OwnerUserId"] = $dataRows[$actualRowsInserted].OwnerUserId
    }
    else
    {
        # the xml does not contains a valid 'OwnerUserId' data, fill the column with null value
        $newPost["OwnerUserId"] = [DBNull]::Value
    }

    # validate the 'FavoriteCount' fields in the xml
    if($dataRows[$actualRowsInserted].FavoriteCount)
    {
        $newPost["FavoriteCount"] = $dataRows[$actualRowsInserted].FavoriteCount
    }
    else
    {
        # the xml does not contains a valid 'FavoriteCount' data, fill the column with null value
        $newPost["FavoriteCount"] = [DBNull]::Value
    }

    if($dataRows[$actualRowsInserted].AnswerCount)
    {
        $newPost["AnswerCount"] = $dataRows[$actualRowsInserted].AnswerCount
    }
    else
    {
        # the xml does not contains a valid 'AnswerCount' data, fill the column with null value
        $newPost["AnswerCount"] = [DBNull]::Value
    }

    if($dataRows[$actualRowsInserted].LastEditDate)
    {
        $newPost["LastEditDate"] = $dataRows[$actualRowsInserted].LastEditDate
    }
    else
    {
        # the xml does not contains a valid 'LastEditDate' data, fill the column with null value
        $newPost["LastEditDate"] = [DBNull]::Value
    }

    if($dataRows[$actualRowsInserted].LastEditorUserId)
    {
        $newPost["LastEditorUserId"] = $dataRows[$actualRowsInserted].LastEditorUserId
    }
    else
    {
        # the xml does not contains a valid 'LastEditorUserId' data, fill the column with null value
        $newPost["LastEditorUserId"] = [DBNull]::Value
    }

    if($dataRows[$actualRowsInserted].OwnerUserId)
    {
        $newPost["OwnerUserId"] = $dataRows[$actualRowsInserted].OwnerUserId
    }
    else
    {
        # the xml does not contains a valid 'OwnerUserId' data, fill the column with null value
        $newPost["OwnerUserId"] = [DBNull]::Value
    }

    if($dataRows[$actualRowsInserted].ViewCount)
    {
        $newPost["ViewCount"] = $dataRows[$actualRowsInserted].ViewCount
    }
    else
    {
        # the xml does not contains a valid 'ViewCount' data, fill the column with null value
        $newPost["ViewCount"] = [DBNull]::Value
    }

    if($dataRows[$actualRowsInserted].AcceptedAnswerId)
    {
        $newPost["AcceptedAnswerId"] = $dataRows[$actualRowsInserted].AcceptedAnswerId
    }
    else
    {
        # the xml does not contains a valid 'AcceptedAnswerId' data, fill the column with null value
        $newPost["AcceptedAnswerId"] = [DBNull]::Value
    }

    if($dataRows[$actualRowsInserted].ParentId)
    {
        $newPost["ParentId"] = $dataRows[$actualRowsInserted].ParentId
    }
    else
    {
        # the xml does not contains a valid 'ParentId' data, fill the column with null value
        $newPost["ParentId"] = [DBNull]::Value
    }

    if($dataRows[$actualRowsInserted].LastAcitvityDate)
    {
        $newPost["LastAcitvityDate"] = $dataRows[$actualRowsInserted].LastAcitvityDate
    }
    else
    {
        # the xml does not contains a valid 'LastAcitvityDate' data, fill the column with null value
        $newPost["LastAcitvityDate"] = [DBNull]::Value
    }

    if($dataRows[$actualRowsInserted].CommunityOwnedDate)
    {
        $newPost["CommunityOwnedDate"] = $dataRows[$actualRowsInserted].CommunityOwnedDate
    }
    else
    {
        # the xml does not contains a valid 'CommunityOwnedDate' data, fill the column with null value
        $newPost["CommunityOwnedDate"] = [DBNull]::Value
    }

    if($dataRows[$actualRowsInserted].ClosedDate)
    {
        $newPost["ClosedDate"] = $dataRows[$actualRowsInserted].ClosedDate
    }
    else
    {
        # the xml does not contains a valid 'ClosedDate' data, fill the column with null value
        $newPost["ClosedDate"] = [DBNull]::Value
    }

    # add the new row to the datatable 
    $postTable.Rows.Add($newPost)

    # increase the counter for actual rows inserted by 1
    $actualRowsInserted +=1

    # check whether the total rows instered has reached the maximum threshold value
    if(($actualRowsInserted -eq $totalRowsToBeInserted-1) -or ($actualRowsInserted % $BatchSize -eq 0))
    {
        pushDataToServer "Posts" $connectionString $postTable
        
        $postTable.Rows.Clear()
        
        Write-Host 'data pushed to server'
        
    }

        # the data table to hold the data to be inserted into post history table
    $postHistoryTable = New-Object System.Data.DataTable

    # define the columns in the posta history datatable
    $postHistoryTable.Columns.Add("Id",[int])
    $postHistoryTable.Columns.Add("Comment")
    $postHistoryTable.Columns.Add("CreationDate",[DateTime])
    $postHistoryTable.Columns.Add("PostHistoryTypeId",[int])
    $postHistoryTable.Columns.Add("PostId",[int])
    $postHistoryTable.Columns.Add("RevisionGUID",[Guid])
    $postHistoryTable.Columns.Add("Text")
    $postHistoryTable.Columns.Add("UserDisplayName")
    $postHistoryTable.Columns.Add("UserId",[int])

    # get the file path to the "PostHistory.xml" file
    $postHistoryFile = $DataDumpLoc+"/PostHistory.xml"

  #  if(-not ())
}

