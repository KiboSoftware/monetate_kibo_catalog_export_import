

## install python dependencies 
```bash
pip3 install -r requirements.txt
```



## setup .env file or pass as --arguments
```console
python process.py --help
```



### errata

#### create cert for monetate api user
```console
openssl rsa -in jwtRS256.key -pubout -outform PEM -out jwtRS256.key.pub
cat jwtRS256.key
cat jwtRS256.key.pub
```
