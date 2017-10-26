import requests
import json
import mimetypes
import simplejson


api_token = None


def main():
    init_file_extension_index()
    inventory_url = "http://localhost:9200/ciber-inventory/_search"
    q_extensions = '''
    {
        "size": 3,
        "aggs": {
            "distinct_format": {
                "terms": {
                    "field": "extension",
                    "size": 500
                },
                "aggs": {
                    "bytes_in_format": {
                        "sum": {
                            "field": "size"
                        }
                    }
                }
            }
        }
    }'''
    headers = {'cache-control': "no-cache"}

    response = requests.request("POST", inventory_url, data=q_extensions, headers=headers).json()

    count = len(response['aggregations']['distinct_format']['buckets'])
    for f in response['aggregations']['distinct_format']['buckets']:
        print '{0} left'.format(count)
        count = count - 1
        # pick several random example files
        q_samples = '''
        {{
            "size": 1,
            "sort" : [
                {{ "random" : {{"order" : "asc"}}}}
            ],
            "query": {{
                "term": {{
                    "extension": "{0}"
                }}
            }}
        }}'''.format(f['key'])
        response = requests.request("POST", inventory_url, data=q_samples, headers=headers).json()
        # extract mimetypes from example files
        sample_filename = response['hits']['hits'][0]['_source']['filename']
        (mimetype, encoding) = mimetypes.guess_type(sample_filename)
        f['mimetype'] = str(mimetype)
        get_token()
        available_extractors = get_extractors(mimetype)
        f['extractors'] = available_extractors
        f['extractors_count'] = len(available_extractors)
        available_conversions = get_conversions(f['key'])
        f['conversions'] = available_conversions
        f['conversions_count'] = len(available_conversions)
        add_file_extension(f['key'], f)


def init_file_extension_index():
    url = "http://localhost:9200/ciber-file-extensions"
    headers = {'cache-control': "no-cache"}
    response = requests.request("DELETE", url, headers=headers)
    payload = '''{
          "mappings": {
            "extension": {
              "properties": {
                "key": {
                  "type": "keyword"
                },
                "conversions": {
                  "type": "keyword"
                }
                "extractors": {
                  "type": "nested",
                  "properties": {
                    "extractor_name": { "type": "text" },
                    "docker_repo": { "type": "text" },
                    "git_repo": { "type": "text" },
                    "extractor_id": { "type": "text" }
                  }
                }
              }
            }
          }
        }'''
    response = requests.request("PUT", url, data=payload, headers=headers)


def add_file_extension(extension, payload):
    url = "http://localhost:9200/ciber-file-extensions/extension/{0}".format(extension)
    response = requests.request("PUT", url, json=payload)
    if 210 <= response.status_code < 200:
        raise Error(response.text)
    print 'Indexed {0}'.format(extension)


def get_extractors(mimetype):
    url = 'https://bd-api.ncsa.illinois.edu/extractors?file_type={0}'.format(mimetype)
    headers = {
        'accept': "application/json",
        'cache-control': "no-cache",
        'authorization': api_token
    }
    resp = requests.request("GET", url, headers=headers)
    return resp.json()


def get_conversions(extension):
    url = 'https://bd-api.ncsa.illinois.edu/conversions/inputs/{0}'.format(extension)
    headers = {
        'accept': "application/json",
        'cache-control': "no-cache",
        'authorization': api_token
    }
    try:
        resp = requests.request("GET", url, headers=headers)
        return resp.json()
    except simplejson.scanner.JSONDecodeError:
        return []


def get_token():
    global api_token
    if api_token is not None:
        return
    url = "https://bd-api.ncsa.illinois.edu/keys/"
    headers = {
        'accept': "application/json",
        'content-type': "application/json",
        'authorization': "Basic amFuc2VuQHVtZC5lZHU6MW1hdHJpeCE=",
        'cache-control': "no-cache"
    }
    api_key = requests.request("POST", url, headers=headers).json()["api-key"]
    url = 'https://bd-api.ncsa.illinois.edu/keys/{0}/tokens'.format(api_key)
    resp = requests.request("POST", url, headers=headers)
    api_token = resp.json()['token']


if __name__ == '__main__':
    main()
