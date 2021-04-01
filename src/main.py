#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
try:
    import os
    import boto3
    import ssl
    import sys
    from boto3 import Session
    from cassandra_sigv4.auth import AuthProvider, Authenticator, SigV4AuthProvider
    from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
    from cassandra.cluster import Cluster
    from cassandra import ConsistencyLevel
    from cassandra.query import SimpleStatement
    import pandas as pd
except ImportError:
    raise RuntimeError('Required packages Failed To install please run "python Setup.py install" command or install '
                       'using pip')

def main():
        ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        cert_path = os.path.join(os.path.dirname(__file__), './sf-class2-root.crt')
        ssl_context.load_verify_locations(cert_path)
        ssl_context.verify_mode = CERT_REQUIRED
        
        # this will automatically pull the credentials from either the
        # ~/.aws/credentials file
        # ~/.aws/config 
        # or from the boto environment variables.
        boto_session = boto3.Session()
        
        # verify that the session is set correctly
        credentials = boto_session.get_credentials()
        
        if not credentials or not credentials.access_key:
            sys.exit("No access key found, please setup credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) according to https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-precedence\n")
    
        
        region = boto_session.region_name
        print('Region is {}'.format(region))

        if not region:  
            sys.exit("You do not have a region set.  Set environment variable AWS_REGION or provide a configuration see https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-precedence\n")
            

        auth_provider = SigV4AuthProvider(boto_session)
        contact_point = "cassandra.{}.amazonaws.com".format(region)

        cluster = Cluster([contact_point], 
                         ssl_context=ssl_context, 
                         auth_provider=auth_provider,
                         port=9142)

        session = cluster.connect()
       
        # Get list of user's keyspaces
        rows = session.execute('select * from system_schema.keyspaces')
        keyspaces = []
        for r in rows.current_rows:  
            if r.keyspace_name in ('system_schema', 'system_schema_mcs', 'system'):
                next
            else:
                print("Found Keyspace: {}".format(r.keyspace_name))
                keyspaces.append(r.keyspace_name)

        df=pd.DataFrame(columns=['region','keyspace','table','column_name','column_kind','column_type'])
        for k in keyspaces:
            tables = session.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = '{}'".format(k))
            print('Keyspace: {}'.format(k))
            for t in tables.current_rows:
                print('  Table: {}'.format(t.table_name))
                table_info = session.execute("SELECT * FROM system_schema.columns WHERE keyspace_name = '{}' AND table_name = '{}'".format(k, t.table_name))
                for i in table_info.current_rows:
                    print('    Column: {} - {} - {}'.format(i.column_name, i.kind, i.type))
                    df = df.append({'region': region, 'keyspace': k, 'table': t.table_name, 'column_name': i.column_name, 'column_kind': i.kind, 'column_type': i.type}, ignore_index=True)
        df.to_excel('./{}.xlsx'.format(region))

if __name__ == '__main__':
    main()

