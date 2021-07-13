import jwt
import requests
import time
import os
import zipfile
import csv
import re
import argparse
from dotenv import load_dotenv
load_dotenv()
parser = argparse.ArgumentParser()
parser.add_argument('--monetate_api_user', required=not os.environ.get('MONETATE_API_USER'),
                    default=os.environ.get('MONETATE_API_USER'))
parser.add_argument('--monetate_api_cert', required=not os.environ.get('MONETATE_API_CERT'),
                    default=os.environ.get('MONETATE_API_CERT'))
parser.add_argument('--monetate_catalog_id', required=not os.environ.get('MONETATE_CATALOG_ID'),
                    default=os.environ.get('MONETATE_CATALOG_ID'))
parser.add_argument('--monetate_account', required=not os.environ.get('MONETATE_ACCOUNT'),
                    default=os.environ.get('MONETATE_ACCOUNT'))
parser.add_argument('--kibo_app_id',
                    required=not os.environ.get('KIBO_APP_ID'),
                    default=os.environ.get('KIBO_APP_ID'))
parser.add_argument('--kibo_app_secrete', required=not os.environ.get('KIBO_APP_SECRETE'),
                    default=os.environ.get('KIBO_APP_SECRETE'))
parser.add_argument('--kibo_api_url', required=not os.environ.get('KIBO_API_URL'),
                    default=os.environ.get('KIBO_API_URL'))
args = parser.parse_args()
kibo_auth_token = False


def main():
    clean()
    latest_monetate_import = get_latest_monetate_import()
    print("latest monetate upload {}".format(latest_monetate_import))
    latest_kibo_import = get_latest_kibo_import()
    print("latest kibo upload {}".format(
        latest_kibo_import if latest_kibo_import["upload_time"] else "Never uploaded"))

    if(latest_kibo_import and latest_kibo_import["upload_time"] == latest_monetate_import["upload_time"]):
        print("up to date")
        return
    zip = process_import(latest_monetate_import)
    file_info = upload_zip(zip, 'catalog.zip')
    import_job = create_import(
        file_info, 'monetate-{}'.format(args.monetate_catalog_id))

    print('import created {}'.format(import_job))

    record_upload(latest_monetate_import["upload_time"])


def clean():
    files = ["s3_temp_download", "kibo_upload.zip", "GoogleProductSpec.csv"]
    for file in files:
        if os.path.exists(file):
            os.remove(file)


def download_file(url):
    local_filename = 's3_temp_download'
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                # if chunk:
                f.write(chunk)
    return local_filename


def convert_to_csv(file):
    file_out = file.split('.')[0] + ".csv"
    with open(file, 'r') as in_str:
        with open(file_out, 'w') as out_str:
            csv_reader = csv.reader(in_str, delimiter='\t')
            csv_writer = csv.writer(out_str)
            for row in csv_reader:
                csv_writer.writerow(row)
    return file_out


def process_import(import_info):
    file = download_file(import_info["s3_url"])
    ext = import_info["upload_filename"].split('.')[-1].lower()
    if (ext == 'tsv'):
        file = convert_to_csv(file)
    os.rename(file, "GoogleProductSpec.csv")
    return create_zip("GoogleProductSpec.csv")


def upload_zip(file, name):
    access_token = get_kibo_auth()
    url = "{}/platform/data/files?fileType=import&fileName={}".format(
        args.kibo_api_url, name)
    auth_header = "Bearer {}".format(access_token["access_token"])

    with open(file, 'rb') as f:
        data = f.read()

    res = requests.post(url=url,
                        data=data,
                        headers={'Authorization': auth_header})
    return res.json()


def record_upload(upload_time):
    url = "{}/platform/entitylists/tenantadminsettings@mozu/entities/latest_import_{}".format(
        args.kibo_api_url, args.monetate_catalog_id)
    access_token = get_kibo_auth()
    auth_header = "Bearer {}".format(access_token["access_token"])
    response = requests.get(url, headers={'Authorization': auth_header})
    if (response.status_code == 200):
        body = response.json()
        body["upload_time"] = upload_time
        res = requests.put(
            url, headers={'Authorization': auth_header}, json=body)
    else:
        url = "{}/platform/entitylists/tenantadminsettings@mozu/entities/".format(
            args.kibo_api_url)
        body = {
            "name": "latest_import_{}".format(args.monetate_catalog_id),
            "upload_time": upload_time
        }
        res = requests.post(
            url, headers={'Authorization': auth_header}, json=body)
    return res.json()


def create_import(file_info, name):
    access_token = get_kibo_auth()
    tenant_id, site_id = get_tenant_site_from_host(args.kibo_api_url)
    tenant = get_kibo_tenant_info(tenant_id)
    mcs = []
    for mc in tenant["masterCatalogs"]:
        for cat in mc["catalogs"]:
            mcs.append(cat)

    site_obj = next(filter(lambda x: x["id"] == site_id, tenant["sites"]))
    master_cat_obj = next(
        filter(lambda x: x["id"] == site_obj["catalogId"], mcs))

    url = "{}/platform/data/import".format(args.kibo_api_url)
    auth_header = "Bearer {}".format(access_token["access_token"])
    body = {
        "name": "monetate",
        "domain": "catalog",
        "resources": [
            {
                "format": "GoogleProductSpec",
                "resource": "GoogleProductSpec"
            }
        ],
        "contextOverride": {
            "site": site_id,
            "masterCatalog": master_cat_obj["id"],
            "locale": site_obj["localeCode"],
            "currency": site_obj["currencyCode"],
            "catalog": site_obj["catalogId"]

        },

        "files": [file_info]
    }
    res = requests.post(url=url,
                        json=body,
                        headers={'Authorization': auth_header})
    return res.json()


def create_zip(file):
    file = 'kibo_upload.zip'
    with zipfile.ZipFile(file, 'w', zipfile.ZIP_DEFLATED) as zip:
        zip.write("GoogleProductSpec.csv")
    return file


def get_kibo_auth():
    global kibo_auth_token
    if (not kibo_auth_token):
        url = "https://home.mozu.com/api/platform/applications/authtickets/oauth"
        body = {
            "client_id": args.kibo_app_id,
            "client_secret": args.kibo_app_secrete,
            "grant_type": "client"
        }
        kibo_auth_token = requests.post(url, json=body).json()
    return kibo_auth_token


def get_tenant_site_from_host(api):
    m = re.search(".*\/t(\d+)-s(\d+)", api)
    return int(m[1]), int(m[2])


def get_kibo_tenant_info(tenant_id):
    url = "https://home.mozu.com/api/platform/tenants/{}".format(tenant_id)
    access_token = get_kibo_auth()
    auth_header = "Bearer {}".format(access_token["access_token"])
    return requests.get(url, headers={'Authorization': auth_header}).json()


def get_latest_kibo_import():
    url = "{}/platform/entitylists/tenantadminsettings@mozu/entities/latest_import_{}".format(
        args.kibo_api_url, args.monetate_catalog_id)
    access_token = get_kibo_auth()
    auth_header = "Bearer {}".format(access_token["access_token"])
    response = requests.get(url, headers={'Authorization': auth_header})
    if (response.status_code == 200):
        return response.json()
    return None


def get_latest_monetate_import():
    imports = get_latest_monetate_imports()
    imports = list(filter(lambda x: x["status"] == "COMPLETE", imports))
    imports = list(
        sorted(imports, key=lambda x: x["upload_time"], reverse=True))
    if(len(imports) > 0):
        return imports[0]
    return None


def get_latest_monetate_imports():
    url = "https://api.monetate.net/api/data/v1/{}/production/import-details/{}/".format(
        args.monetate_account, args.monetate_catalog_id)
    auth_token = get_monetate_token()
    auth_header = "Token {}".format(auth_token)
    imports = requests.get(url,
                           headers={'Authorization': auth_header}).json()
    return imports["data"]


def get_monetate_token():
    private_key = get_private_key()
    payload = jwt.encode({
        'username': args.monetate_api_user,
        'iat': time.time()
    }, private_key, algorithm='RS256')
    auth_header = "JWT {}".format(payload)
    refresh_response = requests.get("https://api.monetate.net/api/auth/v0/refresh/",
                                    headers={'Authorization': auth_header}).json()
    return refresh_response["data"]["token"]


def get_private_key():

    if os.path.isfile(args.monetate_api_cert):
        f = open(args.monetate_api_cert, "rb")
        private_key = f.read()
        f.close()
        return private_key
    return args.monetate_api_cert


if __name__ == "__main__":
    main()
