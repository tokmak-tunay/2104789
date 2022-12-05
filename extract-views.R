DATE_PARAM = '2022-10-26'
date <- as.Date(DATE_PARAM, "%Y-%m-%d")
library(httr)
library(aws.s3)
library(jsonlite)
library(lubridate)
url <- paste(
  'https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia.org/all-access/',
  format(date, "%Y/%m/%d"), sep='')
print(paste('Requesting REST API URL: ', url, sep=''))
wiki.server.response = GET(url)

wiki.response.status = status_code(wiki.server.response)
wiki.response.body = content(wiki.server.response, 'text')

print(paste('Wikipedia REST API Response body: ', wiki.response.body, sep=''))
print(paste('Wikipedia REST API Response Code: ', wiki.response.status, sep=''))


if (wiki.response.status != 200){
  print(paste("Recieved non-OK status code from Wiki Server: ",
              wiki.response.status,
              '. Response body: ',
              wiki.response.body, sep=''
  ))
}


# Save Raw Response and upload to S3
RAW_LOCATION_BASE='data/raw-views'
dir.create(file.path(RAW_LOCATION_BASE), showWarnings = TRUE, recursive = TRUE)

########
# LAB  #
########
#

date <- as.Date(DATE_PARAM, "%Y-%m-%d")
# Save the contents of `wiki.response.body` to file called `raw-views-YYYY-MM-DD.txt` into the folder
#in variable `RAW_LOCATION_BASE` defined 
RAW_LOCATION_BASE='data/raw-views/raw-views'
dir.create(file.path(RAW_LOCATION_BASE), showWarnings = TRUE, recursive = TRUE)


# i.e: `data/raw-views/raw-views-2021-10-01.txt`.

## END OF LAB

########
# LAB  #
########
#
# Upload the file you created to S3.
#
# * Upload the file you created to your bucket (you can reuse your bucket from 
#   the previous classes or create a new bucket. Both solutions work.) 
# * Place the file on S3 into your bucket under a folder called `datalake/raw/`.
# * Don't change the file's name when you are uploading to S3, keep it at `raw-views-YYYY-MM-DD.txt`
# * Once you uploaded the file, verify that it's there (list the bucket in R on the AWS Website)

##### AWS credentials ######

keyTable <- read.csv('keys.csv', header = T) # *accessKeys.csv == the CSV downloaded from AWS containing your Access & Secret keys
AWS_ACCESS_KEY_ID <- as.character(keyTable$Access.key.ID)
AWS_SECRET_ACCESS_KEY <- as.character(keyTable$Secret.access.key)

#activate
Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = "eu-west-1") 


##################################################

RAW_LOCATION_BASE='data/raw-views'
dir.create(file.path(RAW_LOCATION_BASE), showWarnings = TRUE, recursive = TRUE)

write(wiki.response.body, paste0('data/raw-views/raw-views-',DATE_PARAM,'.txt'))

BUCKET <-  'tokmak.tunay'

put_object(file = paste0('C:/Users/HP/Desktop/ECBS-5147-Data-Engineering-2-Big-Data-and-Cloud-Computing/data/raw-views/raw-views-',DATE_PARAM,'.txt'),
           object = paste0('datalake/raw/raw-views-',DATE_PARAM,'.txt'),
           bucket = "tokmak.tunay",
           verbose = TRUE)


## END OF LAB

## Parse the wikipedia response and write the parsed string to "Bronze"

# First, we are extracting the top views from the server's response

wiki.response.parsed = content(wiki.server.response, 'parsed')
top.articles = wiki.response.parsed$items[[1]]$articles

# Convert the server's response to JSON lines
current.time = Sys.time() 
json.lines = ""
for (article in top.articles){
  record = list(
    title = article$article,
    views = article$views,
    rank = article$rank,
    date = format(date, "%Y-%m-%d"),
    retrieved_at = current.time
  )
  
  json.lines = paste(json.lines,
                     toJSON(record,
                            auto_unbox=TRUE),
                     "\n",
                     sep='')
}

# Save the Top views JSON lines as a file and upload it to S3

JSON_LOCATION_BASE='data/views'
dir.create(file.path(JSON_LOCATION_BASE), showWarnings = TRUE)

json.lines.filename = paste("views-", format(date, "%Y-%m-%d"), '.json',
                            sep='')
json.lines.fullpath = paste(JSON_LOCATION_BASE, '/', 
                            json.lines.filename, sep='')

write(json.lines, file = json.lines.fullpath)

put_object(file = json.lines.fullpath,
           object = paste('datalake/views/', 
                          json.lines.filename,
                          sep = ""),
           bucket = BUCKET,
           verbose = TRUE)