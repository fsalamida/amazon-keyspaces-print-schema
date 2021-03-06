## Print Amazon Keyspaces schema

This project prints for each keyspace and table in a said region, the list of columns, their type and kind.
The script will also write the above informations in an excel file.

### Prerequisites
You should have Python and pip installed.  This sample works with Python 2.7, and 3.x.

You should also setup Amazon Keyspaces with an IAM user.  See [Accessing Amazon Keyspaces](https://docs.aws.amazon.com/keyspaces/latest/devguide/accessing.html) for more.

### Running

This script uses Boto which will find credentials based on environment variable, config or credentials file see [Boto3 Credentials Guide](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for how to set this up.  

You can quickly get this to run by explicitly setting the following environment variables...

- AWS_REGION  (for example 'us-east-1)
- AWS_ACCESS_KEY_ID  (ex: 'AKIAIOSFODNN7EXAMPLE')
- AWS_SECRET_ACCESS_KEY (ex: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')

### Install the dependencies 

From the project directory... 
```
$ pip install .
```

### Testing
From this project directory...
```
$ python src/main.py
```

Alternatively you can run the following script after you have done a pip install.

```
$ amazon-keyspaces-print-schema
```