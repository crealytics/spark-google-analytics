CHANGELOG
=========
1.1.2
-----
* Fix a bug where paging returns duplicates

1.1.1
-----
* Supports authentication with Client ID, Client Secret and Refresh Token

1.1.0
-----
* Dimensions don't need to be specified in options anymore, instead just select them in the DataFrame
* Selecting custom dimensions now works
* Update to Spark 2.0.1
* Update to SBT 0.13.13

1.0.0
-----
* Update to Spark 2.0
* Update to Scala 2.11.8 and 2.10.6
* Update to SBT 0.13.12
* Update google-api-services-analytics to v3-rev134-1.22.0
* Update google-http-client-gson to 1.22.0

0.9.0
-----
* New parameter queryIndividualDays can be used to minimize sampling by querying each day from the chosen date range individually
