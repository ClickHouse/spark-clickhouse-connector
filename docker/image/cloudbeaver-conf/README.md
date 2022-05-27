## Generate Admin Password MD5
```
CB_ADMIN_PASSWORD_MD5=`echo -n "$CB_ADMIN_PASSWORD" | md5sum | tr 'a-z' 'A-Z'`
CB_ADMIN_PASSWORD_MD5=${PASSWORD_MD5:0:32}
```

## Authenticate as Admin
```
curl 'http://0.0.0.0:8978/api/gql' \
  -X POST \
  -H 'content-type: application/json' \
  --cookie-jar /tmp/cookie.txt \
  --data '{
    "query": "
        query authLogin($provider: ID!, $credentials: Object!, $linkUser: Boolean) {
            authToken: authLogin(
                provider: $provider
                credentials: $credentials
                linkUser: $linkUser
            ) {
                authProvider
            }
        }
    ",
    "variables": {
        "provider": "local",
        "credentials": {
            "user": "kyuubi",
            "password": "4E212BBF8F138808DB96B969716D1580"
        },
        "linkUser": true
    }
  }'
```

## Expose Connection to Anonymous
```
curl 'http://0.0.0.0:8978/api/gql' \
  -X POST \
  -H 'content-type: application/json' \
  --cookie /tmp/cookie.txt \
  --data '{
    "query": "
        query setConnectionAccess($connectionId: ID!, $subjects: [ID!]!) {
            setConnectionSubjectAccess(connectionId: $connectionId, subjects: $subjects) 
        }
    ",
    "variables": {
        "connectionId": "kyuubi_hive-180f13452e0-749c09a3cdb63869",
        "subjects": ["user"]
    }
  }'
```
