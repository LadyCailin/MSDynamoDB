This extension does not provide any new functions, but instead
provides a new DataSource for the Persistence Network. It allows
connection to an AWS DynamoDB installation.

To use it, you must already have a table set up in AWS, or running locally. The
table must have two columns, "key", which is a string and is the primary key, 
and "value" which is a string. You may set this up directly using whatever means
you like, or alternatively, you can have MethodScript set it up for you, using
the x-msdynamodb-createtable cmdline tool. (Run the commandline tools for
details on the settings and flags.)

Once the table is set up, to use it, set up a routing in the Persistence Network
with the scheme "dynamodb", for example:

    storage.aws.**=dynamodb://url?parameters

The url may be an actual url, (i.e. localhost:8000) which should point to a
self-hosted instance of DynamoDB 
(https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html)
which is useful for testing, but generally will be the special url "aws". This
directs the DataSource to connect to the specified region in the parameters.
The parameters should follow typical url syntax, i.e.

    storage.aws.**=dynamodb://aws?param1=value1&param2=value2

Valid parameters are:

| Parameter       | Description                                               | Default    |
| --------------- | --------------------------------------------------------- | ---------- |
| protocol        | The protocol to use when connecting to non-AWS endpoints  | http       |
| region          | The AWS region to connect to. For non-AWS connections, this is ignored, but is required for AWS connections. May be one of: GovCloud, US_GOV_EAST_1, US_EAST_1, US_EAST_2, US_WEST_1, US_WEST_2, EU_WEST_1, EU_WEST_2, EU_WEST_3, EU_CENTRAL_1, EU_NORTH_1, AP_SOUTH_1, AP_SOUTHEAST_1, AP_SOUTHEAST_2, AP_NORTHEAST_1, AP_NORTHEAST_2, SA_EAST_1, CN_NORTH_1, CN_NORTHWEST_1, or CA_CENTRAL_1 | |
| tableName       | The name of the table to be accessed.                     | &lt;Required&gt; |
| accessKeyId     | The access key id to use. Not required, but if not provided, credentials must be set up in the system itself. | |
| accessKeySecret | The access key secret to use. Only required if accessKeyId is provided | |
| consistentRead  | If "true", reads will be put in the [strongly consistent](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html) read mode. | false |



It is not recommended to use the accessKeyId or accessKeySecret parameters,
instead, you should set the credentials up on the system itself. Instructions
can be found here: 
https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html