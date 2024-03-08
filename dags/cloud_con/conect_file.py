def con_string():
    
    #Im using Aiven for testing with a remote database
    #link:https://aiven.io/
    
    user='your_user'
    pwd='your_pass'
    host='aiven_host'
    port='aiven_port'
    db='test'
    
    return f'postgres://{user}:{pwd}@{host}:{port}/{db}?sslmode=require'